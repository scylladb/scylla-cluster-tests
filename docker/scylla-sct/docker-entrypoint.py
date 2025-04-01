#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import signal
import subprocess
import scyllasetup
import logging
import commandlineparser

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(message)s")

# list of services, previously managed by supervisor, and their start commands
SERVICES = {
    'scylla': '/opt/scylladb/supervisor/scylla-server.sh',
    'scylla-housekeeping': '/scylla-housekeeping-service.sh',
    'scylla-node-exporter': '/opt/scylladb/supervisor/scylla-node-exporter.sh',
    'sshd': '/sshd-service.sh',
    'scylla-manager': '/usr/bin/scylla-manager --developer-mode'
}

processes = {}


def signal_handler(signum, frame):
    for name, proc in processes.items():
        if proc.poll() is None:
            proc.send_signal(signum)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

try:
    arguments, extra_arguments = commandlineparser.parse()
    setup = scyllasetup.ScyllaSetup(arguments, extra_arguments=extra_arguments)
    setup.developerMode()
    setup.cpuSet()
    setup.io()
    setup.cqlshrc()
    setup.arguments()
    setup.set_housekeeping()

    # instead of starting supervisord, start each service directly
    for service_name, command in SERVICES.items():
        if service_name in ['scylla-housekeeping', 'scylla']:
            continue
        processes[service_name] = subprocess.Popen(command, shell=True)

    processes.get('scylla', signal.pause()).wait()

except Exception:  # pylint: disable=broad-except  # noqa: BLE001
    logging.exception('failed!')
