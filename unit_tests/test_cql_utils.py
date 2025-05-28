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
# Copyright (c) 2025 ScyllaDB

from sdcm.utils.cql_utils import cql_quote_if_needed, cql_unquote_if_needed


def test_cql_quote_if_needed():
    assert cql_quote_if_needed('v') == 'v'
    assert cql_quote_if_needed('0') == '"0"'
    assert cql_quote_if_needed('v0') == 'v0'
    assert cql_quote_if_needed('0v') == '"0v"'
    assert cql_quote_if_needed('C') == '"C"'
    assert cql_quote_if_needed('C0') == '"C0"'
    assert cql_quote_if_needed('0C') == '"0C"'
    assert cql_quote_if_needed('column1') == 'column1'
    assert cql_quote_if_needed('columnA') == '"columnA"'
    assert cql_quote_if_needed('1column') == '"1column"'
    assert cql_quote_if_needed('Column1') == '"Column1"'


def test_cql_unquote_if_needed():
    assert cql_unquote_if_needed('"quoted"') == 'quoted'
    assert cql_unquote_if_needed('unquoted') == 'unquoted'
    assert cql_unquote_if_needed('"quoted_left') == '"quoted_left'
    assert cql_unquote_if_needed('quoted_right"') == 'quoted_right"'
    assert cql_unquote_if_needed('quoted_"_middle') == 'quoted_"_middle'
    assert cql_unquote_if_needed('quoted_"_multiple"') == 'quoted_"_multiple"'
    assert cql_unquote_if_needed('quoted_"_multiple"') == 'quoted_"_multiple"'
    assert cql_unquote_if_needed('"quote"_"multiple"') == 'quote"_"multiple'
    assert cql_unquote_if_needed('""') == ''
    assert cql_unquote_if_needed('"') == '"'
