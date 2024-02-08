# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2020 ScyllaDB

import abc
import datetime
import re
from pathlib import Path


class LogTimeConsistencyAnalyzerBase:
    times = {
        '<1min': 60,
        '<5min': 60 * 5,
        '<30min': 60 * 30,
        '<3hours': 60 * 60 * 3,
    }
    lower_shift_limit = 1
    records_limit = 10
    files_pattern = None
    ignore_lines = [
        # Syslog-ng log records that are generated with time shifted
        'syslog-ng starting up',
        'Syslog connection established',
        'Destination reliable queue full',
        # Rsyslog log records that are generated with time shifted
        'rsyslogd:',
        'systemd[',
        'rsyslogd['
    ]

    @classmethod
    @abc.abstractmethod
    def _analyze_file(cls, log_file: Path) -> tuple[dict[str, list[str]], dict[str, int]]:
        pass

    @classmethod
    def _get_timeshift_bucket_name(cls, time_shift: float) -> None | str:
        low_mark = 0
        if time_shift <= cls.lower_shift_limit:
            return None
        for name, high_mark in cls.times.items():
            if low_mark <= time_shift < high_mark:
                return name
            low_mark = high_mark
        return '>3hours'

    @classmethod
    def _init_timeshift_buckets(cls, init_value_type: type):
        return {name: init_value_type() for name in cls.times} | {'>3hours': init_value_type()}

    @classmethod
    def analyze_dir(cls, log_dir: str):
        pathlist = Path(log_dir).glob(cls.files_pattern)
        all_files_data = {}
        total_data = {}
        for log_file in pathlist:
            print('Start processing ' + log_file.parent.name + '/' + log_file.name)
            detailed, counters = cls._analyze_file(log_file)
            all_files_data[str(log_file)] = detailed
            for key, value in counters.items():
                total_data[key] = total_data.get(key, 0) + value
        all_files_data['TOTAL'] = total_data
        return all_files_data

    @classmethod
    def _append_counters_to_details(cls, counters, output):
        for name, value in counters.items():
            if name == 'total':
                continue
            if value > cls.records_limit:
                output[name].append('There are more messages, total number is ' + str(value))


class DbLogTimeConsistencyAnalyzer(LogTimeConsistencyAnalyzerBase):
    files_pattern = '**/*db-node*/messages.log'

    @classmethod
    def _analyze_file(cls, log_file: Path) -> tuple[dict[str, list[str]], dict[str, int]]:
        prior_time = datetime.datetime.now().timestamp() - 60 * 60 * 24 * 365
        output = cls._init_timeshift_buckets(list)
        counters = cls._init_timeshift_buckets(int) | {'total': 0}
        prior_line = ""
        for line in log_file.open(mode='r').readlines():
            if cls.ignore_lines:
                if any(line_pattern in line for line_pattern in cls.ignore_lines):
                    continue
            try:
                current_time = datetime.datetime.fromisoformat(line.split()[0]).timestamp()
            except Exception:  # noqa: BLE001
                continue
            current_time_shift = prior_time - current_time
            if bucket_name := cls._get_timeshift_bucket_name(current_time_shift):
                counters['total'] += 1
                counters[bucket_name] += 1
                if counters[bucket_name] < cls.records_limit:
                    output[bucket_name].append(prior_line + line)
            prior_time = current_time
            prior_line = line
        cls._append_counters_to_details(counters=counters, output=output)
        return output, counters


class SctLogTimeConsistencyAnalyzer(LogTimeConsistencyAnalyzerBase):
    files_pattern = '**/sct.log'
    sct_scylla_log_re = re.compile(
        r'< t:([0-9-]+ [0-9:]+),[0-9]+[ \t]+f:cluster.py[ \t]+l:[0-9]+[ \t]+c:sdcm.cluster[ \t]+p:[A-Z]+ > ([0-9T:-]+)')

    @classmethod
    def _analyze_file(cls, log_file: Path) -> tuple[dict[str, list[str]], dict[str, int]]:
        output = cls._init_timeshift_buckets(list)
        counters = cls._init_timeshift_buckets(int) | {'total': 0}
        for line in log_file.open(mode='r').readlines():
            if cls.ignore_lines:
                if any(line_pattern in line for line_pattern in cls.ignore_lines):
                    continue
            match = cls.sct_scylla_log_re.search(line)
            if not match:
                continue
            # Example:
            # < t:2021-11-09 14:22:18,447 f:cluster.py      l:1405 c:sdcm.cluster   p:DEBUG > 2021-10-06T18:38:00+00:00
            try:
                sct_time, event_time = match.groups()
                sct_time = datetime.datetime.fromisoformat(sct_time).timestamp()
                event_time = datetime.datetime.fromisoformat(event_time).timestamp()
            except Exception:  # noqa: BLE001
                continue
            current_time_shift = sct_time - event_time
            if bucket_name := cls._get_timeshift_bucket_name(time_shift=current_time_shift):
                counters['total'] += 1
                counters[bucket_name] += 1
                if counters[bucket_name] < cls.records_limit:
                    output[bucket_name].append(line)
        cls._append_counters_to_details(counters=counters, output=output)
        return output, counters
