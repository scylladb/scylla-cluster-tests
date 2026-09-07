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

"""Measuring how long a vector-store index took to build, and reporting it to Argus.

Applies to any index vector-store serves -- a full-text ('fulltext_index') or a vector
('vector_index') one -- because the measurement comes from vector-store's own log rather than from
whatever built the index.
"""

import logging
import re
import time
from datetime import datetime

from argus.client.generic_result import ColumnMetadata, ResultType, StaticGenericResultTable, Status

from sdcm.argus_results import submit_results_to_argus

LOGGER = logging.getLogger(__name__)

# How often the vector-store log is re-read while waiting for a finished build's 'full scan' lines.
# Each attempt reads the log from the start, and on the aws backend that is the whole node's
# 'messages.log', which keeps growing over a multi-hour run -- so poll it sparingly. The lines are
# normally there on the first read; this interval only matters when the log shipper lags.
FULL_SCAN_LOG_POLL_INTERVAL_SECS = 2.0
# How long to wait for a finished build's 'full scan' log lines to reach the runner. They are written
# on the vector-store node and forwarded asynchronously, so they can lag the build by a moment; this
# only bounds that lag, it is not a wait for the build itself.
DEFAULT_FULL_SCAN_LOG_WAIT = 120

# Vector-store logs both ends of an index's initial table scan at INFO with a microsecond tracing
# timestamp ("starting/finished full scan on <keyspace>.<index>", see its db_index.rs). That scan is
# the index build, so those two lines are the most direct measurement available -- and the most
# precise. The alternatives are both worse: latte's clock ('latte::now_timestamp()') has
# whole-second resolution, and vector-store's index-status endpoint serves a snapshot refreshed on a
# ~1s ticker, so polling it observes each edge up to a second late.
#
# Two line formats have to be handled, because 'BaseNode.system_log' resolves elsewhere depending on
# 'logs_transport':
#   docker  <node.logdir>/system.log             the raw tracing line
#   aws     <logdir>/hosts/<host>/messages.log   the same line behind a log-shipper prefix
# Only the tracing timestamp is followed by 'Z', which is what tells the two apart.
_FULL_SCAN_RE = re.compile(
    r"(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)Z"
    r".*?\b(?P<event>starting|finished) full scan on (?P<key>\S+)"
)


def index_key(keyspace: str, index_name: str) -> str:
    """Return the '<keyspace>.<index>' key vector-store uses for an index, case-folded.

    Vector-store keys an index by the name it reads out of 'system_schema.indexes', and Scylla
    case-folds unquoted identifiers -- so 'CREATE CUSTOM INDEX fts_idx_10M_20tok_0' is known
    downstream as 'fts_idx_10m_20tok_0'. Anything SCT sends to, or matches against, the vector-store
    API has to be folded the same way, or it silently never matches.
    """
    return f"{keyspace}.{index_name}".lower()


def parse_full_scan_seconds(log_path: str, key: str) -> float | None:
    """Return the duration of *key*'s initial full scan, read from a vector-store log.

    Returns None when the log holds no complete scan for that index -- no matching lines yet (the
    log is shipped asynchronously, so a caller should retry), or a start without a finish. Index
    names are unique per build in the search performance tests, so the first complete start->finish
    pair is the right one.
    """
    key = key.lower()
    started = None
    try:
        with open(log_path, encoding="utf-8", errors="replace") as log_file:
            for line in log_file:
                match = _FULL_SCAN_RE.search(line)
                if not match or match.group("key").lower() != key:
                    continue
                timestamp = datetime.fromisoformat(match.group("ts"))
                if match.group("event") == "starting":
                    started = timestamp
                elif started is not None:
                    return (timestamp - started).total_seconds()
    except OSError:
        return None
    return None


def wait_for_index_build_seconds(
    log_path: str,
    keyspace: str,
    index_name: str,
    timeout: float = DEFAULT_FULL_SCAN_LOG_WAIT,
    poll_interval: float = FULL_SCAN_LOG_POLL_INTERVAL_SECS,
) -> float | None:
    """Return how long an index's initial full scan took, from the vector-store node's log.

    Meant to be called once the build is over, so both log lines already exist on the node -- but
    they reach the runner asynchronously (a tailing thread on docker, a log shipper on aws), so give
    them a bounded window to arrive rather than reading once.

    Normally the first read finds them; the retries are for the shipper lagging, and are paced by
    *poll_interval* because each one re-reads the whole log. Returns None if the lines never turn
    up, which a caller should report as a missing measurement rather than as a failed build.
    """
    key = index_key(keyspace, index_name)
    deadline = time.monotonic() + timeout
    while True:
        build_seconds = parse_full_scan_seconds(log_path, key)
        if build_seconds is not None:
            return build_seconds
        if time.monotonic() >= deadline:
            LOGGER.warning("No complete 'full scan' log pair for '%s' in %s after %ss", key, log_path, timeout)
            return None
        time.sleep(poll_interval)


def index_build_columns(count_column: str, count_unit: str) -> list[ColumnMetadata]:
    """Columns of an index-build Argus table: how long the build took, over how much data.

    *count_column* names what was indexed, in the workload's own vocabulary ('document_count',
    'vector_count'), because that name is what the table's history is keyed by. The table itself is
    declared per workload, so its name and description stay next to the test that owns them.
    """
    return [
        ColumnMetadata(name="build_time", unit="s", type=ResultType.FLOAT, higher_is_better=False),
        ColumnMetadata(name=count_column, unit=count_unit, type=ResultType.INTEGER, higher_is_better=False),
        ColumnMetadata(
            name="indexing_throughput", unit=f"{count_unit}/s", type=ResultType.FLOAT, higher_is_better=True
        ),
    ]


def send_index_build_result(
    argus_client,
    result_table: StaticGenericResultTable,
    count_column: str,
    build_time: float,
    count: int,
    row_key: str,
):
    """Submit one index build row to Argus.

    Argus merges rows into the table it already has under this name, so submitting a single row
    per index build is enough -- the same way 'send_result_to_argus' reports one latency row per
    'row_name' (see performance_regression_alternator_test.py).
    """
    throughput = round(count / build_time, 1) if build_time > 0 and count > 0 else 0.0
    result_table.add_result(column="build_time", row=row_key, value=build_time, status=Status.UNSET)
    result_table.add_result(column=count_column, row=row_key, value=count, status=Status.UNSET)
    result_table.add_result(column="indexing_throughput", row=row_key, value=throughput, status=Status.UNSET)
    submit_results_to_argus(argus_client, result_table)
