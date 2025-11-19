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

import unittest

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
"""


class TestScyllaArgParser(unittest.TestCase):
    def setUp(self):
        self.parser = ScyllaArgParser.from_scylla_help(SCYLLA_HELP_EXAMPLE)

    def test_double_args(self):
        ScyllaArgParser.from_scylla_help(
            SCYLLA_HELP_EXAMPLE + "\n" + SCYLLA_HELP_EXAMPLE,
            duplicate_cb=lambda dups: self.assertEqual(
                ["--help", "--options-file", "--version", "--workdir"], sorted(dups)
            ),
        )

    def test_filter_args(self):
        self.assertEqual(
            self.parser.filter_args(
                "--arg1 arg --version -W arg --arg2 -h --options-file arg",
                unknown_args_cb=lambda args: self.assertEqual(["--arg1", "arg", "--arg2"], args),
            ),
            "--version --workdir arg --help --options-file arg",
        )

    def test_filter_args_unchanged(self):
        args = "--version --workdir arg --help --options-file arg"
        self.assertEqual(self.parser.filter_args(args), args)

    def test_wrong_known_arg(self):
        self.assertRaises(ScyllaArgError, self.parser.filter_args, "-W --arg arg --arg2")
