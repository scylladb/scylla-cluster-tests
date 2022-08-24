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
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel


class UDF(BaseModel):
    name: str
    args: str  # e.g. ((name text, age int)) - needs to be stringified
    called_on_null_input_returns: str  # e.g. int
    return_type: str  # e.g. int
    language: Literal["lua", "xwasm"]
    script: str

    def get_create_query(self, ks: str, create_or_replace: bool = True):
        create_part = "CREATE OR REPLACE" if create_or_replace else "CREATE"
        return f"{create_part} {ks}.{self.name}{self.args} " \
               f"RETURNS {self.called_on_null_input_returns} ON NULL INPUT " \
               f"RETURNS {self.return_type} " \
               f"LANGUAGE {self.language} " \
               f"AS '{self.script}'"

    @classmethod
    def from_yaml(cls, udf_yaml_filename: str):
        with Path(udf_yaml_filename).open(mode="r", encoding="utf-8") as udf_yaml:
            return cls(**yaml.safe_load(udf_yaml))


def _load_all_udfs():
    udfs = {}
    yaml_file_paths = Path("sdcm/utils/udf_scripts").glob("*.yaml")
    for script in yaml_file_paths:
        udf = UDF.from_yaml(str(script))
        udfs.update({udf.name: udf})
    return udfs


UDFS = _load_all_udfs()
