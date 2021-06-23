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

import re
import logging
import argparse
from typing import Text, NoReturn


# Regexp for parsing arguments from the output of `scylla --help' command:
# $ scylla --help
# ...
#   -h [ --help ]                         show help message
#   --version                             print version number and exit
#   --options-file arg                    configuration file (i.e.
# ...
#   -W [ --workdir ] arg                  The directory in which Scylla will put
# ...
from sdcm.sct_events.database import ScyllaHelpErrorEvent

SCYLLA_ARG = \
    re.compile(r"^  (?:(?:(?P<short_arg>-\w) \[ (?P<long_arg>--[\w-]+) \])|(?P<arg>--[\w-]+))(?P<val> arg)?", re.M)

LOGGER = logging.getLogger(__name__)


class ScyllaArgError(Exception):
    pass


class ScyllaArgParser(argparse.ArgumentParser):
    def __init__(self, prog: str) -> None:
        super().__init__(prog=prog, argument_default=argparse.SUPPRESS, add_help=False)

    def error(self, message: Text) -> NoReturn:
        LOGGER.error(message)
        raise ScyllaArgError(message)

    @classmethod
    def from_scylla_help(cls, help: Text) -> "ScyllaArgParser":
        parser = cls(prog="scylla")
        duplicates = set()
        for *args, val in SCYLLA_ARG.findall(help):
            try:
                parser.add_argument(*filter(bool, args), action="store" if val else "store_false")
            except argparse.ArgumentError:
                arg_name = args[0] if len(args) == 1 else args[1]
                duplicates.add(arg_name)
        if duplicates:
            ScyllaHelpErrorEvent.duplicate(
                message=f"Scylla help contains duplicate for the following arguments: {','.join(duplicates)}"
            ).publish()
        return parser

    def filter_args(self, args: str) -> str:
        parsed_args, unknown_args = self.parse_known_args(args.split())
        if unknown_args:
            LOGGER.warning("Following arguments are filtered out: %s", unknown_args)
        filtered_args = []
        for arg, val in vars(parsed_args).items():
            filtered_args.append(f"--{arg.replace('_', '-')}")
            if val:
                filtered_args.append(val)
        return " ".join(filtered_args)
