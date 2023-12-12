#!/usr/bin/env python3
# requires parallel-ssh
# usage python3 parallel_cmd.py <num_hosts> <num_processes> "<command>"
# test by running: ./parallel_cmd.py 20 1 "hostname; echo "
import re
from pathlib import Path

from pssh.clients import ParallelSSHClient
import sys
gce_hosts ="""
perf-regression-z3-ubuntu-loader-node-67e02056-0-5	us-central1-a	Dec 20, 2023, 9:35:05 AM UTC+01:00		
10.128.0.106 (nic0)	34.42.119.211 (nic0) 	
perf-regression-z3-ubuntu-loader-node-67e02056-0-4	us-central1-a	Dec 20, 2023, 9:33:31 AM UTC+01:00		
10.128.0.55 (nic0)	34.170.65.61 (nic0) 	
perf-regression-z3-ubuntu-loader-node-67e02056-0-3	us-central1-a	Dec 20, 2023, 9:32:12 AM UTC+01:00		
10.128.0.27 (nic0)	34.69.252.194 (nic0) 	
perf-regression-z3-ubuntu-loader-node-67e02056-0-2	us-central1-a	Dec 20, 2023, 9:30:43 AM UTC+01:00		
10.128.0.25 (nic0)	34.121.179.96 (nic0) 	
perf-regression-z3-ubuntu-loader-node-67e02056-0-1	us-central1-a	Dec 20, 2023, 9:29:25 AM UTC+01:00		
10.128.0.23 (nic0)	34.123.235.145 (nic0) 
perf-regression-z3-ubuntu-loader-node-dcfe5b34-0-1	us-central1-a	Dec 22, 2023, 5:10:17 AM UTC+01:00		
10.128.0.71 (nic0)	35.193.153.101 (nic0) 
"""


def parse_cs_summary(lines):
    """
    Parsing c-stress results, only parse the summary results.
    Collect results of all nodes and return a dictionaries' list,
    the new structure data will be easy to parse, compare, display or save.
    """
    results = {}
    enable_parse = False

    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith('TAG:'):
            # TAG: loader_idx:1-cpu_idx:0-keyspace_idx:1
            ret = re.findall(r"TAG: loader_idx:(\d+)-cpu_idx:(\d+)-keyspace_idx:(\d+)", line)
            results['loader_idx'] = ret[0][0]
            results['cpu_idx'] = ret[0][1]
            results['keyspace_idx'] = ret[0][2]
            continue
        if line.startswith('Username:'):

            results['username'] = line.split('Username:')[1].strip()
        if line.startswith('Results:'):
            enable_parse = True
            continue
        if line == '':
            continue
        if line == 'END':
            break
        if not enable_parse:
            continue
        split_idx = line.find(':')
        if split_idx < 0:
            continue
        key = line[:split_idx].strip().lower()
        value = line[split_idx + 1:].split()[0].replace(",", "")
        results[key] = value
        match = re.findall(r'\[READ:\s([\d,]+\.\d+)\sms,\sWRITE:\s([\d,]+\.\d)\sms\]', line)
        if match:  # parse results for mixed workload
            results['%s read' % key] = match[0][0]
            results['%s write' % key] = match[0][1]

    if not enable_parse:
        print('Cannot find summary in c-stress results: %s', lines[-10:])
        return {}
    return results

def _convert_stat(stat, stress_result):
    if stat not in stress_result or stress_result[stat] == 'NaN':
        print("Stress stat not found: '%s'", stat)
        return 0
    try:
        return float(stress_result[stat])
    except Exception as details:  # pylint: disable=broad-except
        print("Error in conversion of '%s' for stat '%s': '%s'"
                         "Discarding stat." % (stress_result[stat], stat, details))
    return 0

_stats = {'results': {'stats': []}}

def _calc_stat_total(stat):
    total = 0
    for stress_result in _stats['results']['stats']:
        stat_val = _convert_stat(stat=stat, stress_result=stress_result)
        if not stat_val:
            return 0  # discarding all stat results completely if one of the results is bad
        total += stat_val
    return total

def calculate_stats_average():
    # calculate average stats
    average_stats = {}
    for stat in STRESS_STATS:
        average_stats[stat] = ''  # default
        total = _calc_stat_total(stat=stat)
        if total:
            average_stats[stat] = round(total / len(_stats['results']['stats']), 1)
    _stats['results']['stats_average'] = average_stats

def calculate_stats_total():
    total_stats = {}
    for stat in STRESS_STATS_TOTAL:
        total_stats[stat] = ''  # default
        total = _calc_stat_total(stat=stat)
        if total:
            total_stats[stat] = total
    _stats['results']['stats_total'] = total_stats

hosts = [line.split()[0] for line in gce_hosts.splitlines() if line.strip().startswith('1')]
hosts = hosts[:int(sys.argv[1])]

client = ParallelSSHClient(hosts, pkey='~/.ssh/scylla-test', user='scylla-test')
outputs = []
for i in range(int(sys.argv[2])):
    tag = f"TAG: loader_idx:$(hostname | awk -F- '{{print $NF}}')-cpu_idx:{i}-keyspace_idx:0"
    outputs.append(client.run_command(f"echo {tag}; " + sys.argv[3] + f"-log file=cs-{i}.log interval=10s"))

STRESS_STATS = ('op rate', 'latency mean', 'latency 99th percentile')
STRESS_STATS_TOTAL = ('op rate', 'total errors')

for output in outputs:
    for result in output:
        lines = list(result.stdout) + list(result.stderr)
        Path(lines[0].split(" ")[-1].replace(":", ".")).write_text("\n".join(lines))
        try:
            node_cs_res = parse_cs_summary(lines)  # pylint: disable=protected-access
            if node_cs_res:
                _stats['results']['stats'].append(node_cs_res)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"Failed to process stress summary due to {exc}")


calculate_stats_average()
calculate_stats_total()
print("average: ", _stats['results']['stats_average'])
print("total: ", _stats['results']['stats_total'])
