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

"""Monitoring, events and reporting configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, SctField, String, StringOrList


class MonitoringConfigMixin(BaseModel):
    """Monitoring, events and reporting configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Monitoring, events and reporting"

    monitor_branch: String = SctField(
        description="The port of scylla management",
    )
    sct_ngrok_name: String = SctField(
        description="Override the default hostname address of the sct test runner, using ngrok server, see readme for more instructions",
    )
    backtrace_decoding: Boolean = SctField(
        description="""If True, all backtraces found in db nodes would be decoded automatically""",
    )
    backtrace_stall_decoding: Boolean = SctField(
        description="""If True, reactor stall backtraces will be decoded. If False, reactor stalls are skipped during
         backtrace decoding to reduce overhead in performance tests. Only applies when backtrace_decoding is True.""",
    )
    backtrace_decoding_disable_regex: String = SctField(
        description="""Regex pattern to disable backtrace decoding for specific event types. If an event type matches
         this regex, its backtrace will not be decoded. This can be used to reduce overhead in performance tests
         by skipping backtrace decoding for certain types of events. Only applies when backtrace_decoding is True.""",
    )
    print_kernel_callstack: Boolean = SctField(
        description="""Scylla will print kernel callstack to logs if True, otherwise, it will try and may print a message
         that it failed to.""",
    )
    enable_kernel_panic_checker: Boolean = SctField(
        description="Enable kernel panic detection by monitoring cloud instance console output for panic indicators. "
        "When enabled, a background thread monitors each node's console output for kernel panic patterns.",
    )
    email_recipients: StringOrList = SctField(
        description="""list of email of send the performance regression test to""",
    )
    email_subject_postfix: String = SctField(
        description="""Email subject postfix""",
    )
    max_events_severities: StringOrList = SctField(
        default=[],
        description="Limit severity level for event types",
    )
    scylla_rsyslog_setup: Boolean = SctField(
        description="Configure rsyslog on Scylla nodes to send logs to monitoring nodes",
    )
    events_limit_in_email: int = SctField(
        description="Limit number events in email reports",
    )
    enable_argus: Boolean = SctField(description="Control reporting to argus")
    argus_use_ssh_tunnel: Boolean = SctField(
        description="Enable SSH tunnel support in the Argus client connection",
    )
    download_from_s3: list = SctField(
        description="Destination-source map of dirs/buckets to download from S3 before starting the test",
    )
    argus_email_report_template: String = SctField(
        description="Path to the email report template used for sending argus email reports",
    )
