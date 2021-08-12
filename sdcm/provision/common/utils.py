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
from typing import Any, List, Dict

import attr

from pydantic import BaseModel as PyBaseModel  # pylint: disable=no-name-in-module


__builtin__ = [].__class__.__module__
AttrAttributeType = type(attr.attrib())


def is_builtin(inst):
    return inst.__module__ == __builtin__


def instance_to_dict_without_defaults(inst):
    return attr.asdict(inst, filter=lambda attr_info, value: attr_info.default != value)


def instance_to_dict_with_defaults(inst):
    return attr.asdict(inst)


def as_dict(data) -> Any:  # pylint: disable=too-many-branches
    if hasattr(data, "__attrs_attrs__"):
        fields = attr.fields_dict(type(data))
        output: Dict[str, Any] = {}
        for attr_name in dir(data):
            if attr_name[0] == '_':
                continue
            attr_value = getattr(data, attr_name)
            if attr_annotation := fields.get(attr_name):
                if not attr_annotation.metadata.get('as_dict', True) or attr_value == attr_annotation.default:
                    continue
            if attr_value is None or callable(attr_value):
                continue
            if isinstance(attr_value, (str, float, int)):
                output[attr_name] = attr_value
            else:
                output[attr_name] = as_dict(attr_value)
        return output
    if isinstance(data, (list, tuple)):
        output: List[Any] = [None] * len(data)
        for attr_name, attr_value in enumerate(data):
            if isinstance(attr_value, (str, float, int)):
                output[attr_name] = attr_value
            else:
                output[attr_name] = as_dict(attr_value)
        return output
    if isinstance(data, dict):
        output = {}
        for attr_name, attr_value in data.items():
            if isinstance(attr_value, (str, float, int)):
                output[attr_name] = attr_value
            elif hasattr(attr_value, 'as_dict'):
                output[attr_name] = attr_value.as_dict()
            else:
                output[attr_name] = as_dict(attr_value)
        return output
    raise ValueError("Unexpected data type %s" % (type(data),))


class BaseModel(PyBaseModel):  # pylint: disable=too-few-public-methods
    def __init__(self, **kwargs):
        super().__init__(**{name: value for name, value in kwargs.items() if value is not None})
