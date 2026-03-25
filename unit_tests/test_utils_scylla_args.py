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

from sdcm.utils.scylla_args import ScyllaArgParser, ScyllaArgError


SCYLLA_HELP_EXAMPLE = """\
Scylla version 4.0.0-0.20200505.d95aa77b62 with build-id c9cf14d33523aca01a1ba502a91eb9d400de9c34 starting ...
command used: "/usr/bin/scylla --help"
parsed command line options: [help]
Scylla options:
  -h [ --help ]                         show help message
  --version                             print version number and exit
  --options-file arg                    configuration file (i.e.
                                        <SCYLLA_HOME>/conf/scylla.yaml)
  -W [ --workdir ] arg                  The directory in which Scylla will put
                                        all its subdirectories. The location of
                                        individual subdirs can be overriden by
                                        the respective *_directory options.
  --logger-log-level arg                Set logger log level. Argument is either
                                        'logger=level' or just 'level'. Use
                                        --list-loggers to get the list of
                                        available loggers
"""


@pytest.fixture
def parser():
    return ScyllaArgParser.from_scylla_help(SCYLLA_HELP_EXAMPLE)


def test_double_args():
    captured_dups = []
    ScyllaArgParser.from_scylla_help(
        SCYLLA_HELP_EXAMPLE + "\n" + SCYLLA_HELP_EXAMPLE,
        duplicate_cb=captured_dups.extend,
    )
    assert ["--help", "--logger-log-level", "--options-file", "--version", "--workdir"] == sorted(captured_dups)


def test_filter_args(parser):
    captured_args = []
    result = parser.filter_args(
        "--arg1 arg --version -W arg --arg2 -h --options-file arg",
        unknown_args_cb=captured_args.extend,
    )
    assert result == "--version --workdir arg --help --options-file arg"
    assert captured_args == ["--arg1", "arg", "--arg2"]


def test_filter_args_unchanged(parser):
    args = "--version --workdir arg --help --options-file arg"
    assert parser.filter_args(args) == args


def test_wrong_known_arg(parser):
    with pytest.raises(ScyllaArgError):
        parser.filter_args("-W --arg arg --arg2")


def test_repeated_logger_log_level_args(parser):
    """Test that multiple --logger-log-level arguments are preserved.

    This test demonstrates the issue where only the last occurrence
    of --logger-log-level is kept when multiple are provided.
    """
    # Test case from the issue: multiple --logger-log-level arguments
    result = parser.filter_args("--logger-log-level load_balancer=debug --logger-log-level tablets=debug")

    # Expected: both logger-log-level arguments should be present
    # This assertion will FAIL with the current implementation
    # because argparse with action='store' only keeps the last value
    assert "--logger-log-level load_balancer=debug" in result
    assert "--logger-log-level tablets=debug" in result
