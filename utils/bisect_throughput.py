#!/usr/bin/env python3

# pylint: disable=all
# pip3 install paramiko requests
# TODO: fix upgrade if fails to start scylla - for some builds it freezes, in that case abort upgrade and go to next version,
# also it is prohibited to do downgrade sometimes, so best would be to bootstrap cluster from scratch
# TODO: clean up the code, split to smaller functions (with one purpose), add comments, refactor it

import re
import threading
import time

import paramiko
from typing import List, Tuple

import requests


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


def connect_ssh(host: str, username: str, key_path: str) -> paramiko.SSHClient:
    pkey = paramiko.RSAKey.from_private_key_file(key_path)
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname=host, username=username, pkey=pkey, look_for_keys=False)
    return ssh_client


def upgrade_scylla_binaries(node: str, username: str, key_path: str, repo_file: str, versions: list) -> str:
    print(f"Updating Scylla on node {node} with repo file {repo_file}")
    ssh_client = connect_ssh(node, username, key_path)
    commands = [
        "sudo systemctl stop scylla-server",
        "sudo systemctl stop scylla-node-exporter",
        "sudo find /var/lib/scylla/data/keyspace1/standard1-* -type f | sudo xargs rm -f; sudo rm -rf /var/lib/scylla/commitlog/*",
        f"sudo wget {repo_file} -O /etc/apt/sources.list.d/scylla.list",
        "sudo apt-get update",
        "sudo apt-get remove -y scylla{,-server,-jmx,-tools,-tools-core,-kernel-conf,-node-exporter,-conf,-python3,-cqlsh,-machine-image}",
        "DEBIAN_FRONTEND=noninteractive sudo apt-get install -y -o Dpkg::Options::=\"--force-confold\" scylla{,-server,-jmx,-tools,-tools-core,-kernel-conf,-node-exporter,-conf,-python3,-cqlsh,-machine-image}",
        "sleep 60",
        "sudo systemctl start scylla-node-exporter",
        "sudo systemctl start scylla-server",
        f"until nc -z {node} 9042; do sleep 1; done",
        "scylla --version",
    ]
    for cmd in commands:
        stdin, stdout, stderr = ssh_client.exec_command(cmd)
        stdout.channel.recv_exit_status()
        exit_status = stdout.channel.recv_exit_status()

        if exit_status != 0:
            print(f"Command {cmd} failed with exit status {exit_status}")
            print(f"Stdout: {stdout.read().decode()}")
            print(f"Stderr: {stderr.read().decode()}")
            exit(1)
        if cmd == "scylla --version":
            version = stdout.read().decode().strip()
            versions.append(version)
    print(f"Node {node} updated to Scylla version {version}")
    ssh_client.close()


def analyse_cs_result(output: str) -> float:
    result = parse_cs_summary(output.splitlines())
    print(f"c-s result: {result}")
    try:
        return int(result['op rate'])
    except KeyError:
        print("No op rate found")
        return 0


def run_cassandra_stress(ssh_client: paramiko.SSHClient, loader: str, nodes: list[str], results: dict) -> str:
    print(f"Starting cassandra-stress write on loader {loader}")
    cmd = f"cassandra-stress write no-warmup cl=QUORUM duration=20m -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=3)' -mode cql3 native -rate threads=400 -pop seq=1..30000000 -node {','.join(nodes)}"
    stdin, stdout, stderr = ssh_client.exec_command(cmd)
    # todo: fix possible race condition with results dict update, replace with queues?
    results[loader] = analyse_cs_result(stdout.read().decode())


def bisect_scylla_versions(nodes: List[str], loaders: List[str], repo_file_links: List[str],
                           target_ops: float, username: str, key_path: str) -> Tuple[str, str]:
    low = 0
    high = len(repo_file_links) - 1
    last_good_version = ""
    first_bad_version = ""
    while low <= high:
        mid = (low + high) // 2
        versions = []
        threads = []
        for node in nodes:
            thread = threading.Thread(target=upgrade_scylla_binaries, args=(
                node, username, key_path, repo_file_links[mid], versions))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()
        assert len(set(versions)) == 1, "All nodes should be upgraded to the same version while we have: %s" % versions
        version = versions[0]

        time.sleep(30)  # wait for Scylla to fully stabilise after restart
        results = {}
        threads = []
        for loader in loaders:
            ssh_client = connect_ssh(loader, "centos", key_path)
            thread = threading.Thread(target=run_cassandra_stress, args=(ssh_client, loader, nodes, results))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()
        total_result = sum(results.values())
        print(f"Total result for Scylla version {version}: {total_result}")
        if total_result > target_ops:
            last_good_version = version
            print(f"Version {version} is having acceptable performance. Checking later versions.")
            low = mid + 1
        else:
            first_bad_version = version
            print(f"Version {version} shows performance degradation. Checking earlier versions.")
            high = mid - 1

    return last_good_version, first_bad_version


def prepare_db_nodes(nodes: list[str], username, key_path):
    commands = ["sudo gpg --homedir /tmp --no-default-keyring --keyring /etc/apt/keyrings/scylladb.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys d0a112e067426ab2",
                "sudo apt update",
                "sudo apt install -y netcat wget gpg",
                "sudo rm /etc/systemd/system/scylla-server.service.requires/* || exit 0",
                ]
    for node in nodes:
        ssh_client = connect_ssh(node, username, key_path)
        for cmd in commands:
            stdin, stdout, stderr = ssh_client.exec_command(cmd)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                print(f"Command {cmd} failed with exit status {exit_status}")
                print(f"Stdout: {stdout.read().decode()}")
                print(f"Stderr: {stderr.read().decode()}")
                exit(1)
        ssh_client.close()


# fill out the nodes and loaders ip's and target_ops value
# execute on sct-runner
nodes = ["10.12.8.126", "10.12.10.191", "10.12.8.98"]
loaders = ["10.12.9.238", "10.12.9.236", "10.12.8.54", "10.12.9.63", "10.12.9.55", "10.12.10.246"]
username = "scyllaadm"
key_path = "/home/ubuntu/.ssh/scylla-qa-ec2"
target_ops = 325000.0

repo_file_links = []
# get list of repo files by running:
# aws s3 ls downloads.scylladb.com/unstable/scylla/master/deb/unified/ | tr -s ' ' | cut -d ' ' -f 3 | tr -d '\/'  | sort -g
repo_dates = """
2023-11-22T03:04:23Z
2023-11-23T03:03:59Z
2023-11-24T03:04:01Z
2023-11-24T19:54:09Z
2023-11-25T03:03:42Z
2023-11-27T03:04:20Z
2023-11-28T03:04:49Z
2023-11-29T02:57:42Z
2023-11-30T03:04:40Z
2023-12-02T03:03:48Z
"""
for repo_date in repo_dates.splitlines():
    if not repo_date:
        continue
    link = f"https://downloads.scylladb.com/unstable/scylla/master/deb/unified/{repo_date}/scylladb-master/scylla.list"
    # verify link exists
    res = requests.head(link)
    if res.status_code == 200:
        repo_file_links.append(link)
prepare_db_nodes(nodes, username, key_path)
last_good_version, first_bad_version = bisect_scylla_versions(
    nodes, loaders, repo_file_links, target_ops, username, key_path)
print(f"Last good Scylla version: {last_good_version}")
print(f"First bad Scylla version: {first_bad_version}")
