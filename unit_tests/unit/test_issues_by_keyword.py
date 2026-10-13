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

import pytest

from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.utils.issues_by_keyword.find_known_issue import FindIssuePerBacktrace


def test_find_issue_by_reactor_stall(test_data_dir):
    """A reactor stall backtrace resolves to its known GitHub issue, so reports link the existing bug."""
    backtrace = (test_data_dir / "reactor_stalls_with_known_issue.log").read_text(encoding="utf-8")

    issue_url = FindIssuePerBacktrace().find_issue(
        backtrace_type=DatabaseLogEvent.REACTOR_STALLED.type, decoded_backtrace=backtrace
    )

    assert issue_url == "https://github.com/scylladb/scylladb/issues/8828"


@pytest.mark.parametrize(
    "symbol,expected",
    [
        pytest.param(
            "gms::gossiper::do_send_ack2_msg", "https://scylladb.atlassian.net/browse/SCYLLADB-3114", id="ack2-known"
        ),
        pytest.param(
            "std::vector<seastar::httpd::storage_service_json::snapshot>::push_back",
            "https://scylladb.atlassian.net/browse/SCYLLADB-5320",
            id="snapshot-push-back-known",
        ),
        # the sibling SYN path (jira:SCYLLADB-4852) is a different issue
        pytest.param("gms::gossiper::do_send_ack_msg", None, id="ack-syn-not-linked"),
    ],
)
def test_find_issue_by_oversized_allocation(symbol, expected):
    """An oversized allocation backtrace links to its Jira only for the gossiper ACK2 path it was filed for."""
    backtrace = (
        "seastar::memory::cpu_pages::warn_large_allocation(unsigned long) at ./seastar/src/core/memory.cc:865\n"
        f"(inlined by) {symbol}(utils::tagged_uuid<locator::host_id_tag>, "
        "utils::chunked_vector<gms::gossip_digest, 131072ul>) at ./gms/gossiper.cc:389\n"
        "gms::gossiper::handle_ack_msg(utils::tagged_uuid<locator::host_id_tag>, gms::gossip_digest_ack) "
        "at ./gms/gossiper.cc:334\n"
    )

    assert (
        FindIssuePerBacktrace().find_issue(backtrace_type="OVERSIZED_ALLOCATION", decoded_backtrace=backtrace)
        == expected
    )
