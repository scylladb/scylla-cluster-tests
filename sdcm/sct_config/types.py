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

"""
Custom Pydantic types, input converters and the SctField descriptor used by the SCT configuration.

Note: this module shadows the stdlib ``types`` module for code inside the ``sdcm.sct_config``
package; use absolute imports if you ever need the stdlib one here.
"""

import ast
import pathlib
from distutils.util import strtobool
from typing import List, Union, get_args, get_origin

import yaml
from pydantic import BaseModel, Field, RootModel, fields as pydantic_fields, model_validator
from pydantic.fields import FieldInfo
from pydantic.functional_validators import BeforeValidator
from pydantic.types import confloat
from typing_extensions import Annotated


class IgnoredType:
    pass


def is_ignored_field(field) -> bool:
    """Check if a field is annotated with IgnoredType and should be skipped."""
    return any(isinstance(m, type) and issubclass(m, IgnoredType) for m in getattr(field, "metadata", []))


def _str(value: str | None) -> str | None:
    if value is None:
        return value
    if isinstance(value, str):
        return value
    raise ValueError(f"{value} isn't a string, it is '{type(value)}'")


String = Annotated[str | None, BeforeValidator(_str), Field(json_schema_extra={"appendable": True})]


def _file(value: str) -> str:
    file_path = pathlib.Path(value).expanduser()
    if file_path.is_file() and file_path.exists():
        return value
    raise ValueError(f"{value} isn't an existing file")


ExistingFile = Annotated[str, BeforeValidator(_file)]


def str_or_list_or_eval(value: Union[str, List[str], None]) -> List[str] | None:
    """Convert an environment variable into a Python's list."""

    if value is None:
        return None
    if isinstance(value, str):
        try:
            return ast.literal_eval(value)
        except Exception:  # noqa: BLE001
            pass
        return (
            [
                str(value),
            ]
            if str(value)
            else []
        )

    if isinstance(value, list):
        ret_values = []
        for val in value:
            try:
                ret_values += [ast.literal_eval(val)]
            except Exception:  # noqa: BLE001
                ret_values += [str(val)]
        return ret_values

    raise ValueError(f"{value} isn't a string or a list")


StringOrList = Annotated[
    str | list[str], BeforeValidator(str_or_list_or_eval), Field(json_schema_extra={"appendable": True})
]


def int_or_space_separated_ints(value: str | int | list[int]) -> int | list[int]:
    if value is None:
        return None
    try:
        value = int(value)
        return value
    except Exception:  # noqa: BLE001
        pass

    if isinstance(value, list):
        # Handle list of ints or list of strings that can be converted to ints
        try:
            return [int(v) for v in value]
        except (ValueError, TypeError) as exc:
            raise ValueError(f"{value} isn't a list of integers") from exc

    if isinstance(value, str):
        try:
            values = value.split()
            return [int(v) for v in values]
        except Exception:  # noqa: BLE001
            pass

    raise ValueError("{} isn't int or list".format(value))


IntOrList = Annotated[int | list[int], BeforeValidator(int_or_space_separated_ints)]


def boolean_or_space_separated_booleans(value: bool | list[bool] | str | None) -> bool | list[bool] | None:  # noqa: PLR0911
    """Convert value to a single bool or list of bools.

    Accepts:
    - None -> None
    - bool -> bool
    - list of bools -> list of bools
    - list of strings (true/false/yes/no/1/0) -> list of bools
    - space-separated string of boolean values -> list of bools
    """
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, list):
        if len(value) == 1:
            # Single item list, return just the bool
            if isinstance(value[0], bool):
                return value[0]
            if isinstance(value[0], str):
                return bool(strtobool(value[0]))

        # Handle list of bools or list of strings that can be converted to bools
        try:
            result = []
            for v in value:
                if isinstance(v, bool):
                    result.append(v)
                else:
                    result.append(bool(strtobool(str(v))))
            return result
        except (ValueError, TypeError) as exc:
            raise ValueError(f"{value} isn't a list of booleans") from exc

    if isinstance(value, str):
        try:
            values = value.split()
            if len(values) == 1:
                return bool(strtobool(values[0]))
            return [bool(strtobool(v)) for v in values]
        except Exception:  # noqa: BLE001
            pass

    raise ValueError("{} isn't bool or list".format(value))


BooleanOrList = Annotated[bool | list[bool], BeforeValidator(boolean_or_space_separated_booleans)]


class MultitenantValueMarker:
    """Marker class to identify MultitenantValue types at runtime."""


def is_multitenant_field(field: pydantic_fields.FieldInfo) -> bool:
    """Check if a field uses MultitenantValue type by looking for the marker in its annotation."""
    if not hasattr(field, "annotation") or field.annotation is None:
        return False

    # Check in field.metadata first (where Pydantic stores annotation metadata)
    if hasattr(field, "metadata"):
        for meta in field.metadata:
            if isinstance(meta, MultitenantValueMarker):
                return True

    # Check in annotation args
    def check_annotation(annotation):
        origin = get_origin(annotation)
        if origin is Annotated:
            for arg in get_args(annotation):
                if isinstance(arg, MultitenantValueMarker):
                    return True
                # Recursively check nested annotations
                if check_annotation(arg):
                    return True
        return False

    return check_annotation(field.annotation)


def MultitenantValue(inner_type):  # noqa: N802
    """
    Type wrapper that adds dict[str, T] support to any type T for k8s multitenancy.

    Usage: MultitenantValue[IntOrList], MultitenantValue[StringOrList], etc.

    This allows configuration values to be specified as:
    - Single value: 5
    - List (index-based): [5, 7]
    - Dict (key-based): {tenant1: 5, tenant2: 7}

    The presence of this type automatically indicates multitenancy support,
    eliminating the need for k8s_multitenancy_supported=True flag.
    """
    return Annotated[
        inner_type | dict[str, inner_type],
        MultitenantValueMarker(),
    ]


def dict_or_str(value: dict | str | None) -> dict | None:
    if value is None:
        return None
    elif isinstance(value, str):
        try:
            return ast.literal_eval(value)
        except Exception:  # noqa: BLE001
            pass

        # ast.literal_eval() can fail on some strings (e.g. which contain lowercased booleans), try parsing such strings
        # using yaml.safe_load()
        try:
            return yaml.safe_load(value)
        except Exception:  # noqa: BLE001
            pass

    if isinstance(value, dict):
        return value

    raise ValueError('"{}" isn\'t a dict'.format(value))


DictOrStr = Annotated[dict | str, BeforeValidator(dict_or_str)]


class AdaptiveTimeoutMultipliers(RootModel):
    """Per-operation multipliers for adaptive timeouts.

    Keys must be valid operation names from Operations enum (operation.value[0]),
    e.g. decommission, remove_node, new_node, repair, rebuild, etc.
    Missing keys default to multiplier 1.

    YAML config example::

        adaptive_timeout_multipliers:
          decommission: 4
          new_node: 4
          remove_node: 4

    Environment variable examples:

        SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS="{'decommission': 2, 'new_node': 3}"

    Or using dot-notation (same pattern as SCT_STRESS_IMAGE.*):

        SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS.decommission=4
        SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS.new_node=3

    Or using double-underscore notation (bash-exportable, dots are invalid
    in bash variable names):

        SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS__decommission=4
        SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS__new_node=3
    """

    root: dict[str, confloat(gt=0)] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _validate_operations(cls, value):
        if not isinstance(value, dict):
            return value

        # cyclic-import: Operations imports from sct_config indirectly via cluster
        from sdcm.utils.adaptive_timeouts import Operations  # noqa: PLC0415

        valid_keys = {op.value[0] for op in Operations}
        for key in value.keys():
            if key not in valid_keys:
                raise ValueError(f"Unknown operation key '{key}'. Valid keys: {sorted(valid_keys)}")
        return value

    def get_multiplier(self, operation_key: str) -> float:
        """Return multiplier for the given operation key, or 1.0 if not configured."""
        return float(self.root.get(operation_key, 1.0))


def dict_or_str_or_pydantic(value: dict | str | BaseModel | None) -> dict | BaseModel | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return ast.literal_eval(value)
        except Exception:  # noqa: BLE001
            pass

    if isinstance(value, (dict, BaseModel)):
        return value

    raise ValueError('"{}" isn\'t a dict, str or Pydantic model'.format(value))


DictOrStrOrPydantic = Annotated[dict | str | BaseModel, BeforeValidator(dict_or_str_or_pydantic)]


def _boolean(value):
    if value is None:
        return None
    elif isinstance(value, bool):
        return value
    elif isinstance(value, str):
        return bool(strtobool(value))
    else:
        raise ValueError("{} isn't a boolean".format(type(value)))


Boolean = Annotated[bool, BeforeValidator(_boolean)]


class SctField(FieldInfo):
    """Custom field class for SCT configuration fields.

    This class extends Pydantic's FieldInfo to support SCT-specific metadata.

    Args:
        *args: Positional arguments passed to Pydantic FieldInfo
        **kwargs: Keyword arguments including:
            - description (str): Field description for documentation
            - default: Default value for the field
            - appendable (bool): Whether this field supports the '++' append syntax
                                 in configuration files. When True, values can be
                                 appended using '++value' for strings or ['++', 'value']
                                 for lists. Some types (String, StringOrList) are
                                 appendable by default. Other types like version strings
                                 or region names should set appendable=False.
                                 See merge_dicts_append_strings() for implementation.
            - Other Pydantic Field parameters (validation_alias, etc.)

    Example:
        ```python
        my_field: str = SctField(
            description="Example field",
            appendable=True,  # Allow ++append syntax
        )
        ```
    """

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("default", None)
        extra = {k: v for k, v in kwargs.items() if k in ("appendable",)}
        kwargs.setdefault("json_schema_extra", extra)
        # remove extra keys from kwargs since we moved them to json_schema_extra
        for key in extra:
            kwargs.pop(key, None)
        super().__init__(*args, **kwargs)
