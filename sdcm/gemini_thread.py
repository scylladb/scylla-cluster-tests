import logging
import concurrent.futures
import os
import uuid
import random
import json
import re
import time

from sdcm.sct_events import GeminiEvent, GeminiLogEvent, Severity
from sdcm.utils.common import FileFollowerThread, makedirs

LOGGER = logging.getLogger(__name__)


class NotGeminiErrorResult(object):  # pylint: disable=too-few-public-methods
    def __init__(self, error):
        self.exited = 1
        self.stdout = "n/a"
        self.stderr = str(error)


class GeminiEventsPublisher(FileFollowerThread):
    severity_mapping = {
        "INFO": "NORMAL",
        "DEBUG": "NORMAL",
        "WARN": "WARNING",
        "ERROR": "ERROR",
        "FATAL": "CRITICAL"
    }

    def __init__(self, node, gemini_log_filename):
        super(GeminiEventsPublisher, self).__init__()
        self.gemini_log_filename = gemini_log_filename
        self.node = str(node)
        self.gemini_events = [GeminiLogEvent(
            type='geminievent', regex=r'{"L":"(?P<type>[A-Z]+?)".+"M":"(?P<message>[\w\s.,]+?)"}', severity=Severity.CRITICAL)]

    def run(self):
        patterns = [(event, re.compile(event.regex)) for event in self.gemini_events]

        while True:
            exists = os.path.isfile(self.gemini_log_filename)
            if not exists:
                time.sleep(0.5)
                continue

            for line_number, line in enumerate(self.follow_file(self.gemini_log_filename)):
                for event, pattern in patterns:
                    match = pattern.search(line)
                    if match:
                        data = match.groupdict()
                        event.severity = getattr(Severity, self.severity_mapping[data['type']])
                        event.add_info_and_publish(node=self.node, line=data['message'], line_number=line_number)
                if self.stopped():
                    break
            if self.stopped():
                break


class GeminiStressThread(object):  # pylint: disable=too-many-instance-attributes

    def __init__(self, test_cluster, oracle_cluster, loaders, gemini_cmd, timeout=None, outputdir=None):  # pylint: disable=too-many-arguments
        self.loaders = loaders
        self.gemini_cmd = gemini_cmd
        self.test_cluster = test_cluster
        self.oracle_cluster = oracle_cluster
        self.timeout = timeout
        self.result_futures = []
        self.gemini_log = None
        self.outputdir = outputdir
        self.gemini_commands = []

    def _generate_gemini_command(self, loader_idx):
        seed = random.randint(1, 100)
        test_node = random.choice(self.test_cluster.nodes)
        oracle_node = random.choice(self.oracle_cluster.nodes)
        self.gemini_log = '/tmp/gemini-l{}-{}.log'.format(loader_idx, uuid.uuid4())
        cmd = "/$HOME/{} --test-cluster={} --oracle-cluster={} --outfile {} --seed {}".format(self.gemini_cmd.strip(),
                                                                                              test_node.ip_address,
                                                                                              oracle_node.ip_address,
                                                                                              self.gemini_log,
                                                                                              seed)
        self.gemini_commands.append(cmd)
        return cmd

    def run(self):

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(self.loaders.nodes))
        for loader_idx, loader in enumerate(self.loaders.nodes):
            self.result_futures.append(executor.submit(self._run_gemini, loader, loader_idx))

        return self

    def _run_gemini(self, node, loader_idx):
        logdir = os.path.join(self.outputdir, node.name)
        makedirs(logdir)

        log_file_name = os.path.join(logdir,
                                     'gemini-l%s-%s.log' %
                                     (loader_idx, uuid.uuid4()))
        gemini_cmd = self._generate_gemini_command(loader_idx)

        GeminiEvent(type='start', cmd=gemini_cmd)
        try:
            with GeminiEventsPublisher(node=node, gemini_log_filename=log_file_name):

                result = node.remoter.run(cmd=gemini_cmd,
                                          timeout=self.timeout,
                                          ignore_status=False,
                                          log_file=log_file_name)
                # sleep to gather all latest log messages
                time.sleep(5)
        except Exception as details:  # pylint: disable=broad-except
            LOGGER.error(details)
            result = getattr(details, "result", NotGeminiErrorResult(details))

        GeminiEvent(type='finish', cmd=gemini_cmd,
                    result={'exit_code': result.exited,
                            'stdout': result.stdout,
                            'stderr': result.stderr})

        return node, result, self.gemini_log

    def get_gemini_results(self):
        results = []
        command_result = []

        LOGGER.debug('Wait for %s gemini threads results', len(self.loaders.nodes))
        for future in concurrent.futures.as_completed(self.result_futures, timeout=self.timeout):
            results.append(future.result())

        for node, _, result_file in results:

            local_gemini_result_file = os.path.join(node.logdir, os.path.basename(result_file))
            node.remoter.receive_files(src=result_file, dst=local_gemini_result_file)
            with open(local_gemini_result_file) as local_file:
                content = local_file.read()
                res = self._parse_gemini_summary_json(content)
                if res:
                    command_result.append(res)

        return command_result

    @staticmethod
    def verify_gemini_results(results):

        stats = {'status': None, 'results': [], 'errors': {}}
        if not results:
            LOGGER.error('Gemini results are not found')
            stats['status'] = 'FAILED'
        else:
            for res in results:
                stats['results'].append(res)
                for err_type in ['write_errors', 'read_errors', 'errors']:
                    if err_type in res.keys() and res[err_type]:
                        LOGGER.error("Gemini {} errors: {}".format(err_type, res[err_type]))
                        stats['status'] = 'FAILED'
                        stats['errors'][err_type] = res[err_type]
        if not stats['status']:
            stats['status'] = "PASSED"

        return stats

    @staticmethod
    def _parse_gemini_summary_json(json_str):
        results = {'result': {}}
        try:
            results = json.loads(json_str)

        except Exception as details:  # pylint: disable=broad-except
            LOGGER.error("Invalid json document {}".format(details))

        return results.get('result')

    @staticmethod
    def _parse_gemini_summary(lines):
        results = {}
        enable_parse = False

        for line in lines:
            line.strip()
            if 'Results:' in line:
                enable_parse = True
                continue
            if "run completed" in line:
                enable_parse = False
                continue
            if not enable_parse:
                continue

            split_idx = line.index(':')
            key = line[:split_idx].strip()
            value = line[split_idx + 1:].split()[0]
            results[key] = int(value)
        return results
