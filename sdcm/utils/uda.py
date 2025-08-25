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
from typing import Optional

import yaml
from pydantic import BaseModel

from sdcm.utils.udf import UDF, UDFS


class UDA(BaseModel):
    """
    Provides the representation for User Defined Aggregates in SCT,
    and the interface for creating queries for them.

    User Defined Aggregates work very much the same as regular CQL aggregate functions,
    and can be defined via CQL, e.g. :

        CREATE FUNCTION accumulate_len(acc tuple<bigint,bigint>, a text)
        RETURNS NULL ON NULL INPUT
        RETURNS tuple<bigint,bigint>
        LANGUAGE lua as 'return {acc[1] + 1, acc[2] + #a}';

        CREATE OR REPLACE FUNCTION present(res tuple<bigint,bigint>)
        RETURNS NULL ON NULL INPUT
        RETURNS text
        LANGUAGE lua as 'return "The average string length is " .. res[2]/res[1] .. "!"';

        CREATE OR REPLACE AGGREGATE avg_length(text)
        SFUNC accumulate_len
        STYPE tuple<bigint,bigint>
        FINALFUNC present INITCOND (0,0);

    (Optionally you can also add a REDUCEFUNC <reduce_func_name> if running a
    map-reduce style distributed workload.)
    As UDAs are made up of UDFs, the referenced UDFs need to be created before attempting
    to create a UDA.
    UDAs are bound to keyspaces, i.e. they cannot be used outside the keyspace they were
    created in.

    Using the custom UDA is similar to usual CQL functions. Assuming we're using the keyspace
    the UDA was created in:

        SELECT avg_length(word) FROM words;

    UDF/UDA presentation from 2019 summit: https://www.scylladb.com/tech-talk/udf-uda-and-whats-in-the-future/
    UDF/UDA testing desing doc: https://docs.google.com/document/d/16GTe1bLmMBC5IVCjC_nY-UNnMCLr_3V2C6K6tiYPstQ
    /edit?usp=sharing
    """
    name: str
    args: str
    return_type: str
    accumulator_udf: UDF
    reduce_udf: Optional[UDF]
    final_udf: UDF
    initial_condition: str

    def get_create_query_string(self, ks: str) -> str:
        query_string = f"CREATE AGGREGATE {ks}.{self.name}" \
            f"({self.args}) SFUNC {self.accumulator_udf.name} " \
            f'STYPE {self.accumulator_udf.return_type} '
        if self.reduce_udf:
            query_string += f"REDUCEFUNC {self.reduce_udf.name} "

        query_string += f"FINALFUNC {self.final_udf.name} " \
            f"INITCOND {self.initial_condition};"
        return query_string

    @classmethod
    def from_yaml(cls, uda_yaml_file_path: str) -> UDA:
        with Path(uda_yaml_file_path).open(mode="r", encoding="utf-8") as uda_yaml:
            input_yaml = yaml.safe_load(uda_yaml)
            accumulator_udf = UDFS.get((input_yaml.get("accumulator_udf_name", None)))
            reduce_udf = UDFS.get((input_yaml.get("reduce_udf_name", None)))
            final_udf = UDFS.get((input_yaml.get("final_udf_name", None)))
            return UDA(
                accumulator_udf=accumulator_udf,
                reduce_udf=reduce_udf,
                final_udf=final_udf,
                **input_yaml
            )


def _load_all_udas() -> dict[str, UDA]:
    """Convenience functions for loading all the existing UDA scripts from /sdcm/utils/udas"""
    udas = {}
    yaml_file_paths = Path("sdcm/utils/udas").glob("*.yaml")
    for script in yaml_file_paths:
        uda = UDA.from_yaml(str(script))
        udas.update({script.stem: uda})
    return udas


UDAS = _load_all_udas()
