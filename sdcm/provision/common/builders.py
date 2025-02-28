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

from typing import Any

from pydantic import BaseModel, ConfigDict  # pylint: disable=no-name-in-module


class AttrBuilder(BaseModel):

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def model_dump(
        self,
        *,
        exclude_none: bool = True,
        **kwargs,
    ) -> dict[str, Any]:
        # we want just to default to exclude_none=True
        return super().model_dump(
            exclude_none=exclude_none,
            **kwargs,
        )
