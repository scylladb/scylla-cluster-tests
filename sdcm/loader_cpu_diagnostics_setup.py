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
# Copyright (c) 2026 ScyllaDB

"""Loader CPU/thread diagnostics sampler (SCT-601).

A client-observed P99 spike with flat server-side metrics used to be impossible to attribute: the
only loader-side signal was the coarse `node_load1` of node_exporter. This installs a 1 Hz sampler
on the loader host that records per-CPU utilization (including `%steal`), PSI CPU pressure, per
process and - optionally - per thread CPU of the stress tool, plus its context switches.

The sampler is a systemd service so it covers the whole run regardless of how many stress threads
come and go on the loader, and is `Restart=always` so a crashed sampler does not silently stop
producing data. Its log is streamed off the loader live by
:class:`~sdcm.utils.remote_logger.LoaderCpuFileLogger` (perf loaders get terminated, and the
interesting samples are exactly the last ones) and collected by ``LoaderLogCollector``.
"""

import logging

from sdcm.remote import shell_script_cmd

LOGGER = logging.getLogger(__name__)

SAMPLER_SCRIPT_NAME = "loader_cpu_sampler.sh"
REMOTE_SCRIPT_PATH = f"/usr/local/bin/{SAMPLER_SCRIPT_NAME}"
# /var/tmp and not /tmp: systemd-tmpfiles will not clean it up under a long run
REMOTE_LOG_PATH = "/var/tmp/loader-cpu.log"
# the local file name LoaderLogCollector picks up, per loader (node.logdir)
LOCAL_LOG_NAME = "loader-cpu.log"
SERVICE_NAME = "loader-cpu-diag"

SAMPLE_INTERVAL = 1  # seconds
# per-thread sampling every Nth sample only: a c-s run has ~1000 threads, one line each
THREAD_EVERY_N_SAMPLES = 10
TOP_THREADS = 20
MAX_LOG_SIZE_MB = 2048


class LoaderCpuDiagnosticsSetup:
    """Install and start the loader CPU diagnostics sampler on a loader node."""

    @staticmethod
    def install(node: "BaseNode", per_thread: bool = False) -> None:  # noqa: F821
        """Install sysstat, upload the sampler and start it as a systemd service.

        `sysstat` gives the sampler mpstat/pidstat; it is best-effort on purpose - the sampler falls
        back to reading /proc directly, and diagnostics must never fail a loader setup.
        """
        node.log.info("Setting up loader CPU diagnostics sampler")
        try:
            node.install_package("sysstat", ignore_status=True)
        except Exception:  # noqa: BLE001
            node.log.warning("Failed to install sysstat, the sampler will fall back to /proc", exc_info=True)
        if not node.remoter.run("command -v pidstat", ignore_status=True).ok:
            node.log.warning("pidstat is not available on %s, the sampler falls back to /proc counters", node.name)

        # imported here and not at module level: sdcm.utils.common is heavy and this module is
        # imported by sdcm.utils.remote_logger, which sits below it in the import order
        from sdcm.utils.common import get_data_dir_path  # noqa: PLC0415

        node.remoter.send_files(get_data_dir_path(SAMPLER_SCRIPT_NAME), f"/tmp/{SAMPLER_SCRIPT_NAME}")
        thread_every = THREAD_EVERY_N_SAMPLES if per_thread else 0
        # kept on one line: the script below is dedent()ed, a continuation line would break the heredoc
        exec_start = (
            f"{REMOTE_SCRIPT_PATH} -i {SAMPLE_INTERVAL} -o {REMOTE_LOG_PATH} "
            f"-t {thread_every} -T {TOP_THREADS} -m {MAX_LOG_SIZE_MB}"
        )
        node.remoter.sudo(
            shell_script_cmd(f"""
            install -m 0755 /tmp/{SAMPLER_SCRIPT_NAME} {REMOTE_SCRIPT_PATH}

            cat <<EOM > /etc/systemd/system/{SERVICE_NAME}.service
            [Unit]
            Description=SCT loader CPU diagnostics sampler
            After=network.target

            [Service]
            Type=simple
            # pin the sysstat output format, see the sampler script
            Environment=S_TIME_FORMAT=ISO LC_ALL=C
            ExecStart={exec_start}
            Restart=always
            RestartSec=5
            # slightly favoured over the stress tool so that samples keep coming while the loader is
            # saturated - which is exactly when they matter. One wakeup per second, no measurable cost.
            Nice=-5

            [Install]
            WantedBy=multi-user.target
            EOM

            systemctl daemon-reload
            systemctl enable {SERVICE_NAME}.service
            systemctl restart {SERVICE_NAME}.service
        """),
            retry=3,
        )
        node.log.info("Loader CPU diagnostics sampler started, logging to %s", REMOTE_LOG_PATH)
        LoaderCpuDiagnosticsSetup._verify_sampling(node)

    @staticmethod
    def _verify_sampling(node: "BaseNode") -> None:  # noqa: F821
        """Warn if the service is not actually sampling.

        `systemctl restart` of a Type=simple unit succeeds as soon as the process is forked, so a
        sampler that dies immediately (missing interpreter, bad option, unwritable log) looks
        installed and leaves the run without any diagnostics - the very failure this is here to
        prevent. Only a warning: a missing sampler must not fail a test.
        """
        if not node.remoter.run(f"systemctl is-active {SERVICE_NAME}.service", ignore_status=True).ok:
            node.log.warning(
                "Loader CPU diagnostics service is not active on %s, no samples will be collected", node.name
            )
            return
        # the first sample lands one interval after start, give it a couple of them
        samples = node.remoter.run(
            f"sleep {SAMPLE_INTERVAL * 3}; grep -c 'sample=' {REMOTE_LOG_PATH}", ignore_status=True
        )
        if not samples.ok or samples.stdout.strip() in ("", "0"):
            node.log.warning(
                "Loader CPU diagnostics service is running on %s but wrote no sample yet to %s",
                node.name,
                REMOTE_LOG_PATH,
            )
