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
from unittest import TestCase

from pydantic import ValidationError

from sdcm.utils.udf import UDF, UDFS


class TestUDF(TestCase):
    MOCK_LUA_UDF_VALS = {
        "name": "var_length_counter",
        "args": "(var text)",
        "called_on_null_input_returns": "NULL",
        "return_type": "int",
        "language": "lua",
        "script": "return #var"
    }

    MOCK_XWASM_UDF_VALS = {
        "name": 'plus',
        "args": '(input1 tinyint, input2 tinyint)',
        "called_on_null_input_returns": 'NULL',
        "return_type": 'tinyint',
        "language": 'xwasm',
        "script": r"""(module
(type (;0;) (func (param i32 i32) (result i32)))
(func ${plus_name} (type 0) (param i32 i32) (result i32)
  local.get 1
  local.get 0
  i32.add)
(table (;0;) 1 1 funcref)
(table (;1;) 32 externref)
(memory (;0;) 17)
(export "memory" (memory 0))
(export "{plus_name}" (func ${plus_name}))
(elem (;0;) (i32.const 0) func)
(global (;0;) i32 (i32.const 1024))
(export "_scylla_abi" (global 0))
(data $.rodata (i32.const 1024) "\\01"))"""
    }

    def test_create_udf_instance(self):
        expected_vals = self.MOCK_LUA_UDF_VALS

        udf = UDF(**expected_vals)

        for key, value in expected_vals.items():
            self.assertEqual(value, getattr(udf, key), f"Did not find expected value for {key} in the udf class.")

    def test_get_create_query_from_udf(self):
        expected_query = "CREATE mock_keyspace.var_length_counter(var text) RETURNS NULL ON NULL INPUT " \
                         "RETURNS int LANGUAGE lua AS 'return #var'"
        udf = UDF(**self.MOCK_LUA_UDF_VALS)
        actual_query = udf.get_create_query(ks="mock_keyspace", create_or_replace=False)

        self.assertEqual(expected_query, actual_query)

    def test_get_create_or_replace_query_from_udf(self):
        expected_query = "CREATE OR REPLACE mock_keyspace.var_length_counter(var text) RETURNS NULL ON NULL INPUT " \
                         "RETURNS int LANGUAGE lua AS 'return #var'"
        udf = UDF(**self.MOCK_LUA_UDF_VALS)
        actual_query = udf.get_create_query(ks="mock_keyspace")

        self.assertEqual(expected_query, actual_query)

    def test_creating_udf_with_missing_required_argument(self):
        required_arg_names = ["name", "args", "called_on_null_input_returns", "return_type", "script"]

        for arg_name in required_arg_names:
            udf_args = self.MOCK_LUA_UDF_VALS.copy()
            udf_args.update({arg_name: None})

            with self.assertRaises(ValidationError,
                                   msg=f"Creating a udf without providing {arg_name} did not raise a ValidationError."):
                UDF(**udf_args)

    def test_creating_udf_class_with_invalid_language(self):
        udf_vals = self.MOCK_LUA_UDF_VALS.copy()
        udf_vals.update({"language": "Java"})

        with self.assertRaises(ValidationError,
                               msg="Creating UDF class with invalid language did not raise ValidationError."):
            UDF(**udf_vals)

    def test_loading_udfs_with_lua_scripts(self):
        expected_vals = self.MOCK_LUA_UDF_VALS.copy()

        udf_yaml_filename = "./sdcm/utils/udf_scripts/lua_var_length_counter.yaml"
        udf = UDF.from_yaml(udf_yaml_filename)

        self.assertIsNotNone(udf)

        for key, value in expected_vals.items():
            self.assertEqual(value, getattr(udf, key), f"Did not find expected value for {key} in the udf class.")

    def test_loading_udfs_with_xwasm_scripts(self):
        expected_vals = self.MOCK_XWASM_UDF_VALS.copy()

        udf_yaml_filename = "./sdcm/utils/udf_scripts/xwasm_plus.yaml"
        udf = UDF.from_yaml(udf_yaml_filename)

        self.assertIsNotNone(udf)

        for key, value in expected_vals.items():
            self.assertEqual(value, getattr(udf, key), f"Did not find expected value for {key} in the udf class.")

    def test_load_all_udfs(self):
        self.assertGreater(len(UDFS.keys()), 1, "UDF count was not greater than 1.")
        for udf in UDFS.values():
            self.assertTrue(udf.name)
            self.assertTrue(udf.args)
            self.assertTrue(udf.called_on_null_input_returns)
            self.assertTrue(udf.return_type)
            self.assertTrue(udf.language)
            self.assertTrue(udf.script)
