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
# Copyright (c) 2024 ScyllaDB

import re


def cql_quote_if_needed(identifier: str) -> str:
    """
    quote cql identifier if needed

    cql identifiers that start with a digit, or that aren't lower case ascii
    should be quoted

    https://cassandra.apache.org/doc/stable/cassandra/cql/definitions.html#identifiers
    """
    identifier_regex = re.compile(r'^[^0-9][a-z0-9_]+$')
    if not identifier_regex.match(identifier):
        return f'"{identifier}"'
    return identifier


def cql_unquote_if_needed(identifier: str):
    """
    This function only removes the quotes if both the first and last characters
    of the cql_response are double quotes. This ensures that any quotes inside the
    cql_response remain intact.

    For example:
      - '"5gb_sizetiered_6_1"' becomes '5gb_sizetiered_6_1'
      - '5gb_sizetiered_6_1' remains unchanged.

    Args:
        identifier (str): Raw CQL response that might be quoted.

    Returns:
        str: The unquoted identifier if the outer quotes are present, otherwise the original response.
    """
    if identifier and len(identifier) >= 2 and identifier.startswith('"') and identifier.endswith('"'):
        return identifier[1:-1]
    return identifier
