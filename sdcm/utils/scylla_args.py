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
# Copyright (c) 2021 ScyllaDB

import re
import logging
import argparse
from typing import Text, NoReturn, Callable


# Regexp for parsing arguments from the output of `scylla --help' command:
# $ scylla --help
# ...
#   -h [ --help ]                         show help message
#   --version                             print version number and exit
#   --options-file arg                    configuration file (i.e.
# ...
#   -W [ --workdir ] arg                  The directory in which Scylla will put
# ...
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
    def from_scylla_help(cls, help_text: Text, duplicate_cb: Callable = None) -> "ScyllaArgParser":
        parser = cls(prog="scylla")
        duplicates = set()
        for *args, val in SCYLLA_ARG.findall(help_text):
            try:
                parser.add_argument(*filter(bool, args), action="store" if val else "store_false")
            except argparse.ArgumentError:
                if arg_names := list(filter(bool, args)):
                    duplicates.add(arg_names[-1])
        if duplicates and duplicate_cb:
            duplicate_cb(duplicates)

        return parser

    def filter_args(self, args: str, unknown_args_cb: Callable = None) -> str:
        parsed_args, unknown_args = self.parse_known_args(args.split())
        if unknown_args and unknown_args_cb:
            unknown_args_cb(unknown_args)
        filtered_args = []
        for arg, val in vars(parsed_args).items():
            filtered_args.append(f"--{arg.replace('_', '-')}")
            if val:
                filtered_args.append(val)
        return " ".join(filtered_args)
