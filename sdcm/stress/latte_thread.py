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
# Copyright (c) 2023 ScyllaDB

import contextlib
import logging
import os
import posixpath
import re
import shlex
import threading
import uuid
from pathlib import Path

from sdcm.loader import (
    LatteExporter,
    LatteHDRExporter,
    LatteKeyspaceHolder,
)
from sdcm.prometheus import nemesis_metrics_obj
from sdcm.provision.helpers.certificate import SCYLLA_SSL_CONF_DIR, TLSAssets
from sdcm.remote.libssh2_client.exceptions import Failure
from sdcm.reporting.tooling_reporter import LatteVersionReporter
from sdcm.sct_events import Severity
from sdcm.sct_events.loaders import LatteStressEvent
from sdcm.stress.base import DockerBasedStressThread
from sdcm.utils.common import get_sct_root_path
from sdcm.utils.docker_remote import RemoteDocker
from sdcm.utils.remote_logger import HDRHistogramFileLogger

LATTE_FN_NAME_RE = r"(?:-f|--function)[ =]([\w\s\d:,]+)|--functions[ =]([\w\s\d:,]+)"
LATTE_TAG_RE = r"--tag(?:\s+|=)([\w-]+(?:,[\w-]+)*)\b"
LOGGER = logging.getLogger(__name__)


def find_latte_fn_names(stress_cmd):
    fn_names = []
    matches = re.findall(LATTE_FN_NAME_RE, stress_cmd)
    for group in matches:
        for item in group:
            if not item.strip():
                continue
            # 'write:1,read:2'
            sub_items = item.split(",")
            for sub_item in sub_items:
                # 'write' and 'read'
                fn_names.append(sub_item.split(":")[0].strip())
    return fn_names


def find_latte_tags(stress_cmd):
    tags = []
    matches = re.findall(LATTE_TAG_RE, stress_cmd)
    for item in matches:
        if not item.strip():
            continue
        sub_items = item.split(",")
        for sub_item in sub_items:
            tags.append(sub_item.strip())
    return tags


def get_latte_operation_type(stress_cmd):
    write_found, counter_write, read_found, counter_read = False, False, False, False
    for fn in find_latte_fn_names(stress_cmd):
        if fn == "counter_write":
            counter_write = True
        elif fn == "counter_read":
            counter_read = True
        elif re.findall(r"(?:^|_)(write|insert|update|delete)(?:_|$)", fn):
            write_found = True
        elif re.findall(r"(?:^|_)(read|select|get|count|search)(?:_|$)", fn):
            read_found = True
        else:
            return "user"
    if write_found and not (counter_write or counter_read or read_found):
        return "write"
    elif read_found and not (counter_write or counter_read or write_found):
        return "read"
    elif counter_write and not (write_found or read_found or counter_read):
        return "counter_write"
    elif counter_read and not (write_found or read_found or counter_write):
        return "counter_read"
    else:
        return "mixed"


# What this run sent to loader hosts: (loader name, host path) -> (local path, size, mtime) of the
# file sent there. Only these count as sent -- a file an earlier run left on a host may hold anything.
_SENT_TO_LOADERS: dict[tuple[str, str], tuple[str, int, int]] = {}
_SEND_LOCKS: dict[tuple[str, str], threading.Lock] = {}
_LOADER_HOMES: dict[str, str] = {}
_LOADER_FILES_LOCK = threading.Lock()
# Keeps apart the runs (and test workers) that share a host and loader names -- a docker host, say.
_STAGING_RUN = uuid.uuid4().hex[:8]


def loader_path(loader, relative: str) -> str:
    """Where *relative* lives on *loader*'s host, under 'latte-staging/<run>/<loader name>' in its
    home.

    Docker loaders all run on one host; the loader name keeps their files apart. The home is
    resolved, not written as '$HOME': rsync sends the destination with '--secluded-args', so no
    remote shell expands it.
    """
    with _LOADER_FILES_LOCK:
        home = _LOADER_HOMES.get(loader.name)
    if home is None:
        home = loader.remoter.run("echo $HOME", verbose=False).stdout.strip()
        if not home.startswith("/"):
            raise RuntimeError(f"Cannot resolve the home directory on {loader.name}: got {home!r}")
        with _LOADER_FILES_LOCK:
            _LOADER_HOMES[loader.name] = home
    return posixpath.join(home, "latte-staging", _STAGING_RUN, loader.name, relative.lstrip("/"))


def _fingerprint(local_path: str) -> tuple[str, int, int]:
    stat = os.stat(local_path)
    return local_path, stat.st_size, stat.st_mtime_ns


def _send_lock(key: tuple[str, str]) -> threading.Lock:
    with _LOADER_FILES_LOCK:
        return _SEND_LOCKS.setdefault(key, threading.Lock())


def send_to_loader(loader, files: list[tuple[str, str]]) -> None:
    """Send '(local_path, host_path)' pairs to *loader*'s host, skipping a file this run already sent
    there unchanged.
    """
    pending = [
        (local_path, host_path)
        for local_path, host_path in files
        if _SENT_TO_LOADERS.get((loader.name, host_path)) != _fingerprint(local_path)
    ]
    if not pending:
        return
    parents = sorted({posixpath.dirname(host_path) for _, host_path in pending})
    loader.remoter.run(f"mkdir -p {' '.join(map(shlex.quote, parents))}", verbose=False)
    for local_path, host_path in pending:
        key = (loader.name, host_path)
        with _send_lock(key):
            fingerprint = _fingerprint(local_path)
            if _SENT_TO_LOADERS.get(key) == fingerprint:
                continue
            # One call per file: the local and agent runners take a single path, not a list.
            if not loader.remoter.send_files(local_path, host_path, verbose=False):
                raise RuntimeError(f"Failed to send {local_path} to {loader.name}:{host_path}")
            _SENT_TO_LOADERS[key] = fingerprint


class LatteStressThread(DockerBasedStressThread):
    DOCKER_IMAGE_PARAM_NAME = "stress_image.latte"
    SCHEMA_CMD_CALL_COUNTER = {}

    def __init__(self, *args, extra_files_to_stage=None, record_hdr=True, **kwargs):
        """*extra_files_to_stage* is a list of '(local_path, container_path)' pairs only this
        invocation needs, such as a dataset shard. 'build_stress_cmd' copies them into the loader
        container; the container is removed after the run.

        *record_hdr* set to False skips the HDR histogram of this invocation even when
        'use_hdrhistogram' is on, for a command whose latency nobody reads.
        """
        super().__init__(*args, **kwargs)
        self.extra_files_to_stage: list[tuple[str, str]] = extra_files_to_stage or []
        self.record_hdr = record_hdr
        for _, container_path in self.extra_files_to_stage:
            if not container_path.startswith("/"):
                raise ValueError(f"Container path {container_path!r} of a staged file is not absolute")

    def records_hdr(self) -> bool:
        # 'latte schema' takes no '--hdrfile'
        return self.record_hdr and bool(self.params.get("use_hdrhistogram")) and "latte schema" not in self.stress_cmd

    def set_stress_operation(self, stress_cmd):
        return get_latte_operation_type(self.stress_cmd)

    def _script_name(self) -> str:
        return re.search(r"([/\w-]*\.rn)", self.stress_cmd).group(0)

    def _build_ssl_config(self, cmd_runner, loader):
        """Build SSL configuration flags and upload SSL files to the loader."""
        if not self.params["client_encrypt"]:
            return ""

        for ssl_file in loader.ssl_conf_dir.iterdir():
            if ssl_file.is_file():
                cmd_runner.send_files(str(ssl_file), str(SCYLLA_SSL_CONF_DIR / ssl_file.name), verbose=True)

        ssl_config = (
            f" --ssl --ssl-ca {SCYLLA_SSL_CONF_DIR}/{TLSAssets.CA_CERT} "
            f"--ssl-cert {SCYLLA_SSL_CONF_DIR}/{TLSAssets.CLIENT_CERT} "
            f"--ssl-key {SCYLLA_SSL_CONF_DIR}/{TLSAssets.CLIENT_KEY}"
        )

        if self.params["peer_verification"]:
            ssl_config += " --ssl-peer-verification"

        return ssl_config

    def _build_datacenter_rack_flags(self, loader):
        """Build --datacenter and --rack flags for topology-aware routing."""
        datacenter, rack = "", ""
        if self.params.get("rack_aware_loader"):
            rack_names = self.loader_set.get_rack_names_per_datacenter_and_rack_idx(db_nodes=self.node_list)
            if len(set(rack_names.values())) > 1:
                if loader_rack := rack_names.get((str(loader.region), str(loader.rack))):
                    rack = f"--rack {loader_rack} "
        if self.loader_set.test_config.MULTI_REGION or rack:
            # The datacenter name can be received from "nodetool status" output. It's possible for DB nodes only,
            # not for loader nodes. So call next function for DB nodes
            datacenter_name_per_region = self.loader_set.get_datacenter_name_per_region(db_nodes=self.node_list)
            if loader_dc := datacenter_name_per_region.get(loader.region):
                datacenter = f"--datacenter {loader_dc} "
            else:
                LOGGER.error(
                    "Not found datacenter for loader region '%s'. Datacenter per loader dict: %s",
                    loader.region,
                    datacenter_name_per_region,
                )
                if rack:
                    # NOTE: fail fast if we cannot find proper dc value when rack-awareness is enabled
                    raise RuntimeError(f"Could not find proper dc-pair for the loader rack value: {rack}")
        return datacenter, rack

    def _script_dir_mount(self, loader) -> str:
        """Send the top-level files of the rune script's directory to the loader host, and return the
        docker option mounting them where the command looks for the script.

        Every latte call gets a container of its own, so copying them into the container cost a
        transfer per file per call; on the host they are sent once, and again only if changed.
        """
        script_dir = Path(self._script_name()).parent
        host_dir = loader_path(loader, f"scripts/{script_dir}")
        files = sorted(path for path in (Path(get_sct_root_path()) / script_dir).iterdir() if path.is_file())
        send_to_loader(loader, [(str(path), f"{host_dir}/{path.name}") for path in files])
        return "-v " + shlex.quote(f"{host_dir}:/{str(script_dir).lstrip('/')}:ro,z")

    def build_stress_cmd(self, cmd_runner, loader, hosts):
        script_name = self._script_name()
        if script_name not in self.SCHEMA_CMD_CALL_COUNTER:
            self.SCHEMA_CMD_CALL_COUNTER[script_name] = 0

        for local_path, container_path in self.extra_files_to_stage:
            if not cmd_runner.send_files(local_path, container_path, verbose=False):
                raise RuntimeError(f"Failed to copy {local_path} into the loader container at {container_path}")

        ssl_config = self._build_ssl_config(cmd_runner, loader)

        auth_config = ""
        if credentials := self.loader_set.get_db_auth():
            auth_config = f" --user {credentials[0]} --password {credentials[1]}"

        datacenter, rack = self._build_datacenter_rack_flags(loader)

        custom_schema_params = ""
        if latte_schema_parameters := self.params["latte_schema_parameters"]:
            # NOTE: string parameters in latte must be wrapped into escaped double-quotes: foo="\"bar\""
            for k, v in latte_schema_parameters.items():
                processed_v = v
                try:
                    processed_v = int(v)
                except Exception:  # noqa: BLE001
                    if v not in ("true", "false"):
                        processed_v = r"\"%s\"" % v
                custom_schema_params += f" -P {k}={processed_v}"
        first_tag_or_op = (find_latte_tags(self.stress_cmd) or [self.stress_operation])[0]
        # NOTE: use superuser creds for the 'latte schema' cmd because test users will only be created with it.
        schema_cmd = f"latte schema {script_name} {ssl_config}{auth_config}{custom_schema_params} -- {hosts}"
        # NOTE: allow schema creation repetition for non-argus usages (unit and integration tests)
        if LatteStressThread.SCHEMA_CMD_CALL_COUNTER[script_name] < 1 or not self.params.get("enable_argus"):
            LOGGER.debug("Calling following 'latte schema' (tag: %s) cmd: %s", first_tag_or_op, schema_cmd)
            result = cmd_runner.run(cmd=schema_cmd, timeout=self.timeout, retry=0)
            try:
                LOGGER.debug(
                    "Output for 'latte schema' (tag: %s) cmd\nstdout: %s\nstderr: %s",
                    first_tag_or_op,
                    result.stdout,
                    result.stderr,
                )
            except Exception as e:  # noqa: BLE001
                LOGGER.error("Failed to print out the results of the `latte schema` command. e: %s", e)
            # NOTE: it should not require any locking because practically we have multiple seconds
            #       diff reaching this code out by different stress threads.
            LatteStressThread.SCHEMA_CMD_CALL_COUNTER[script_name] += 1
            tester = self.loader_set.test_config.tester_obj()
            if hasattr(tester, "run_post_latte_schema_cmd"):
                tester.run_post_latte_schema_cmd()
        else:
            LOGGER.debug("Skip calling following 'latte schema' (tag: %s) cmd: %s", first_tag_or_op, schema_cmd)

        # NOTE: set '--user' and '--password' params only if not defined explicitly
        if " --user" in self.stress_cmd and " --password" in self.stress_cmd:
            auth_config = ""
        # NOTE: 'latte schema' doesn't support '-q', but other subcommands (e.g. 'latte run') do.
        quiet_flag = "" if "latte schema" in self.stress_cmd else "-q "
        stress_cmd = f"{self.stress_cmd} {ssl_config}{auth_config}{custom_schema_params} {datacenter}{rack}{quiet_flag}"
        self.set_hdr_tags(self.stress_cmd)

        return stress_cmd

    def set_hdr_tags(self, stress_cmd):
        # NOTE: latte HDR histogram tags are constructed like "fn--foo" where 'foo' is a rune function name.
        #       There will be HDR tags for every rune function used in a stress command.
        self.hdr_tags = [f"fn--{fn_name}" for fn_name in find_latte_fn_names(stress_cmd)]

    @staticmethod
    def parse_final_output(result):
        """
        Parse latte final results to match what we get out of cassandra-stress
        latencies returned in milliseconds

        :param result: output of latte stats
        :return: dict
        """
        ops_regex = re.compile(r"\s*Throughput(.*?)\[op\/s\]\s*(?P<op_rate>\d*)\s")
        latency_99_regex = re.compile(r"\s* 99 \s*(?P<latency_99th_percentile>\d*\.\d*)\s")
        latency_mean_regex = re.compile(
            r"\s*(?:Mean resp\. time|Request latency)\s*(?:\[(ms|s)\])?\s*(?P<latency_mean>\d+\.\d+)"
        )

        output = {"latency 99th percentile": 0, "latency mean": 0, "op rate": 0}
        for line in result.stdout.split("SUMMARY STATS")[-1].splitlines():
            if match := ops_regex.match(line):
                output["op rate"] = match.groupdict()["op_rate"]
                continue
            if match := latency_99_regex.match(line):
                output["latency 99th percentile"] = float(match.groupdict()["latency_99th_percentile"])
                continue
            if match := latency_mean_regex.match(line):
                output["latency mean"] = float(match.groupdict()["latency_mean"])
                continue

        # output back to strings
        output = {k: str(v) for k, v in output.items()}
        return output

    def _run_stress(self, loader, loader_idx, cpu_idx):
        cpu_options = ""
        if self.stress_num > 1:
            cpu_options = f'--cpuset-cpus="{cpu_idx}"'

        if not os.path.exists(loader.logdir):
            os.makedirs(loader.logdir, exist_ok=True)

        first_tag_or_op = "-" + (find_latte_tags(self.stress_cmd) or [self.stress_operation])[0]
        log_file_name = os.path.join(
            loader.logdir, "latte%s-l%s-c%s-%s.log" % (first_tag_or_op, loader_idx, cpu_idx, uuid.uuid4())
        )
        LOGGER.debug("latte benchmarking tool local log: %s", log_file_name)

        # TODO: fix usage of the "$HOME". Code works when home is "/". It will fail for non-root.
        log_id = self._build_log_file_id(loader_idx, cpu_idx, "")
        remote_hdr_file_name = f"hdrh-latte-{self.stress_operation}-{log_id}.hdr"
        LOGGER.debug("latte remote HDR histogram log file: %s", remote_hdr_file_name)
        local_hdr_file_name = os.path.join(loader.logdir, remote_hdr_file_name)
        LOGGER.debug("latte HDR local file %s", local_hdr_file_name)

        loader.remoter.run(f"touch $HOME/{remote_hdr_file_name}", ignore_status=False, verbose=False)
        remote_hdr_file_name_full_path = loader.remoter.run(
            f"realpath $HOME/{remote_hdr_file_name}",
            ignore_status=False,
            verbose=False,
        ).stdout.strip()
        cmd_runner = cleanup_context = RemoteDocker(
            loader,
            self.docker_image_name,
            command_line="-c 'tail -f /dev/null'",
            extra_docker_opts=(
                "--network=host "
                "--security-opt seccomp=unconfined "
                f"--entrypoint /bin/bash {cpu_options} --label shell_marker={self.shell_marker}"
                f" -v {remote_hdr_file_name_full_path}:/{remote_hdr_file_name}:z"
                # The script path in the command is relative, so it resolves against the workdir.
                f" --workdir / {self._script_dir_mount(loader)}"
            ),
        )
        hosts = " ".join([i.cql_address for i in self.node_list])
        stress_cmd = self.build_stress_cmd(cmd_runner, loader, hosts)
        if self.records_hdr():
            stress_cmd += f" --hdrfile={remote_hdr_file_name}"
            hdrh_logger_context = HDRHistogramFileLogger(
                node=loader,
                remote_log_file=remote_hdr_file_name_full_path,
                target_log_file=os.path.join(loader.logdir, remote_hdr_file_name),
            )
            # NOTE: running dozens of commands in parallel on a single SCT runner
            #       it is easy to get stress command to run earlier than the HDRH file
            #       starts being read.
            #       So, to avoid data loss by time mismatch we start reading earlier
            #       to make sure we do not race with stress threads start time.
            hdrh_logger_context.start()
        else:
            hdrh_logger_context = contextlib.nullcontext()
        stress_cmd += f" -- {hosts} "

        try:
            LatteVersionReporter(
                runner=cmd_runner,
                command_prefix=stress_cmd.split("latte", maxsplit=1)[0],
                argus_client=loader.parent_cluster.test_config.argus_client(),
            ).report()
        except Exception:  # noqa: BLE001
            LOGGER.info("Failed to collect latte version information", exc_info=True)

        LOGGER.debug("running: %s", stress_cmd)
        result, keyspace_holder = {}, LatteKeyspaceHolder()
        with (
            cleanup_context,
            hdrh_logger_context,
            LatteExporter(
                keyspace=keyspace_holder,
                instance_name=loader.ip_address,
                metrics=nemesis_metrics_obj(),
                stress_operation=self.stress_operation,
                stress_log_filename=log_file_name,
                loader_idx=loader_idx,
                cpu_idx=cpu_idx,
            ),
            LatteHDRExporter(
                keyspace=keyspace_holder,
                instance_name=loader.ip_address,
                hdr_tags=self.hdr_tags,
                metrics=nemesis_metrics_obj(),
                stress_operation=self.stress_operation,
                stress_log_filename=local_hdr_file_name,
                loader_idx=loader_idx,
                cpu_idx=cpu_idx,
            ),
            LatteStressEvent(node=loader, stress_cmd=stress_cmd, log_file_name=log_file_name) as latte_stress_event,
        ):
            try:
                result = cmd_runner.run(
                    cmd=stress_cmd,
                    timeout=self.timeout + self.shutdown_timeout,
                    log_file=log_file_name,
                    retry=0,
                    timestamp_logs=True,
                )
                result = self.parse_final_output(result)
            except Exception as exc:  # noqa: BLE001
                self.configure_event_on_failure(stress_event=latte_stress_event, exc=exc)

        return loader, result, latte_stress_event

    def configure_event_on_failure(self, stress_event: LatteStressEvent, exc: Exception | Failure):
        error_msg = format_stress_cmd_error(exc)
        if (hasattr(exc, "result") and exc.result.failed) and exc.result.exited == 137:
            error_msg = f"Stress killed by test/teardown\n{error_msg}"
            stress_event.severity = Severity.WARNING
        elif self.stop_test_on_failure:
            stress_event.severity = Severity.CRITICAL
        else:
            stress_event.severity = Severity.ERROR
        stress_event.add_error(errors=[error_msg])


def format_stress_cmd_error(exc: Exception) -> str:
    """Format nicely the exception from a stress command failure."""
    if hasattr(exc, "result") and exc.result.failed:
        # NOTE: print only last 2 lines in common case or whole 'panic' message
        last_n_lines, line_index = exc.result.stderr.splitlines()[-30:], -2
        for current_line_index, line in enumerate(last_n_lines):
            if "panicked at" in line:
                line_index = current_line_index
                break
        message = "\n".join(last_n_lines[line_index:])
        return f"Stress command completed with bad status {exc.result.exited}: {message}"
    return f"Stress command execution failed with: {exc}"
