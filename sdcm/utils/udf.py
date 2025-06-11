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
# Copyright (c) 2022 ScyllaDB
from __future__ import annotations
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel


class UDF(BaseModel):
    """
    Provides the representation for User Defined Functions in SCT,
    and the interface for creating queries for them.

    User Defined Functions work very much the same as regular CQL functions, and can be defined via CQL,
    e.g. :

        CREATE FUNCTION accumulate_len(acc tuple<bigint,bigint>, a text)
        RETURNS NULL ON NULL INPUT
        RETURNS tuple<bigint,bigint>
        LANGUAGE lua as 'return {acc[1] + 1, acc[2] + #a}';

    The scripting languages supported as of 2022.1 / 5.0 are:
    - Lua
    - WASM

    UDF/UDA presentation from 2019 summit: https://www.scylladb.com/tech-talk/udf-uda-and-whats-in-the-future/
    UDFs initial commit: https://github.com/scylladb/scylladb/commit/1fe062aed4b1ceb5a97b4333ae6c1901854a7f39
    WASM support design doc: https://github.com/scylladb/scylladb/blob/master/docs/dev/wasm.md
    WASM blog post: https://www.scylladb.com/2022/04/14/wasmtime/
    UDF/UDA testing desing doc: https://docs.google.com/document/d/16GTe1bLmMBC5IVCjC_nY-UNnMCLr_3V2C6K6tiYPstQ/edit?usp=sharing
    """
    name: str
    args: str
    called_on_null_input_returns: str
    return_type: str
    language: Literal["lua", "wasm"]
    script: str

    def get_create_query(self, ks: str, create_or_replace: bool = True) -> str:
        create_part = "CREATE OR REPLACE FUNCTION" if create_or_replace else "CREATE FUNCTION"
        return f"{create_part} {ks}.{self.name}{self.args} " \
            f"RETURNS {self.called_on_null_input_returns} ON NULL INPUT " \
            f"RETURNS {self.return_type} " \
            f"LANGUAGE {self.language} " \
            f"AS '{self.script}'"

    @classmethod
    def from_yaml(cls, udf_yaml_filename: str) -> UDF:
        with Path(udf_yaml_filename).open(mode="r", encoding="utf-8") as udf_yaml:
            return cls(**yaml.safe_load(udf_yaml))


def _load_all_udfs() -> dict[str, UDF]:
    """Convenience functions for loading all the existing UDF scripts from /sdcm/utils/udf_scripts"""
    udfs = {}
    yaml_file_paths = Path("sdcm/utils/udf_scripts").glob("*.yaml")
    for script in yaml_file_paths:
        udf = UDF.from_yaml(str(script))
        udfs.update({script.stem: udf})
    return udfs


UDFS = _load_all_udfs()
