# Monitoring, events and reporting

[← All configuration options](configuration_options.md)

The monitoring stack, event severities, Argus reporting and email reports.

**17 options.** Jump to: [argus_email_report_template](#argus_email_report_template) · [argus_use_ssh_tunnel](#argus_use_ssh_tunnel) · [backtrace_decoding](#backtrace_decoding) · [backtrace_decoding_disable_regex](#backtrace_decoding_disable_regex) · [backtrace_stall_decoding](#backtrace_stall_decoding) · [download_from_s3](#download_from_s3) · [email_recipients](#email_recipients) · [email_subject_postfix](#email_subject_postfix) · [enable_argus](#enable_argus) · [enable_kernel_panic_checker](#enable_kernel_panic_checker) · [events_limit_in_email](#events_limit_in_email) · [max_events_severities](#max_events_severities) · [monitor_branch](#monitor_branch) · [monitor_swap_size](#monitor_swap_size) · [print_kernel_callstack](#print_kernel_callstack) · [sct_ngrok_name](#sct_ngrok_name) · [scylla_rsyslog_setup](#scylla_rsyslog_setup)


## **argus_email_report_template** / SCT_ARGUS_EMAIL_REPORT_TEMPLATE

Path to the email report template used for sending argus email reports

**default:** email_report_template_basic.yaml

**type:** str (appendable)


## **argus_use_ssh_tunnel** / SCT_ARGUS_USE_SSH_TUNNEL

Enable SSH tunnel support in the Argus client connection

**default:** True

**type:** bool


## **backtrace_decoding** / SCT_BACKTRACE_DECODING

If True, all backtraces found in db nodes would be decoded automatically

**default:** True

**type:** bool

**backend overrides:**
- `False`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce


## **backtrace_decoding_disable_regex** / SCT_BACKTRACE_DECODING_DISABLE_REGEX

Regex pattern to disable backtrace decoding for specific event types. If an event type matches<br>this regex, its backtrace will not be decoded. This can be used to reduce overhead in performance tests<br>by skipping backtrace decoding for certain types of events. Only applies when backtrace_decoding is True.

**default:** N/A

**type:** str (appendable)


## **backtrace_stall_decoding** / SCT_BACKTRACE_STALL_DECODING

If True, reactor stall backtraces will be decoded. If False, reactor stalls are skipped during<br>backtrace decoding to reduce overhead in performance tests. Only applies when backtrace_decoding is True.

**default:** True

**type:** bool


## **download_from_s3** / SCT_DOWNLOAD_FROM_S3

Destination-source map of dirs/buckets to download from S3 before starting the test

**default:** []

**type:** list


## **email_recipients** / SCT_EMAIL_RECIPIENTS

list of email of send the performance regression test to

**default:** ['qa@scylladb.com']

**type:** str | list[str] → list[str] (appendable)


## **email_subject_postfix** / SCT_EMAIL_SUBJECT_POSTFIX

Text appended to the subject of the test result email, to tell similar runs apart.

**default:** N/A

**type:** str (appendable)


## **enable_argus** / SCT_ENABLE_ARGUS

Control reporting to argus

**default:** True

**type:** bool


## **enable_kernel_panic_checker** / SCT_ENABLE_KERNEL_PANIC_CHECKER

Enable kernel panic detection by monitoring cloud instance console output for panic indicators. When enabled, a background thread monitors each node's console output for kernel panic patterns.

**default:** True

**type:** bool


## **events_limit_in_email** / SCT_EVENTS_LIMIT_IN_EMAIL

Maximum number of events of each severity to include in the email report.

**default:** 10

**type:** int


## **max_events_severities** / SCT_MAX_EVENTS_SEVERITIES

Limit severity level for event types

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **monitor_branch** / SCT_MONITOR_BRANCH

The port of scylla management

**default:** branch-4.16

**type:** str (appendable)

**backend overrides:**
- `N/A`: aws, gce, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **monitor_swap_size** / SCT_MONITOR_SWAP_SIZE

The size of the swap file for the monitors. Its size in bytes calculated by x * 1MB

**default:** N/A

**type:** int


## **print_kernel_callstack** / SCT_PRINT_KERNEL_CALLSTACK

Scylla will print kernel callstack to logs if True, otherwise, it will try and may print a message<br>that it failed to.

**default:** True

**type:** bool

**backend overrides:**
- `False`: docker


## **sct_ngrok_name** / SCT_SCT_NGROK_NAME

DEPRECATED (see SCT-954, unused for years): expose the SCT runner under this ngrok hostname instead of its own address.

**default:** N/A

**type:** str (appendable)


## **scylla_rsyslog_setup** / SCT_SCYLLA_RSYSLOG_SETUP

Configure rsyslog on Scylla nodes to send logs to monitoring nodes

**default:** False

**type:** bool
