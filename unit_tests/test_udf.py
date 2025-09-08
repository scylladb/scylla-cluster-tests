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
        "name": "lua_var_length_counter",
        "args": "(var text)",
        "called_on_null_input_returns": "NULL",
        "return_type": "int",
        "language": "lua",
        "script": "return #var",
    }

    MOCK_WASM_UDF_VALS = {
        "name": "wasm_plus",
        "args": "(input1 int, input2 int)",
        "called_on_null_input_returns": "NULL",
        "return_type": "int",
        "language": "wasm",
        "script": r"""(module
(type (;0;) (func))
(type (;1;) (func (param i32 i32) (result i32)))
(func $__wasm_call_ctors (type 0))
(func $plus (type 1) (param i32 i32) (result i32)
  local.get 1
  local.get 0
  i32.add)
(table (;0;) 1 1 funcref)
(memory (;0;) 2)
(global (;0;) (mut i32) (i32.const 66560))
(global (;1;) i32 (i32.const 1024))
(global (;2;) i32 (i32.const 1024))
(global (;3;) i32 (i32.const 1024))
(global (;4;) i32 (i32.const 66560))
(global (;5;) i32 (i32.const 0))
(global (;6;) i32 (i32.const 1))
(export "memory" (memory 0))
(export "__wasm_call_ctors" (func $__wasm_call_ctors))
(export "wasm_plus" (func $plus))
(export "__dso_handle" (global 1))
(export "__data_end" (global 2))
(export "__global_base" (global 3))
(export "__heap_base" (global 4))
(export "__memory_base" (global 5))
(export "__table_base" (global 6)))""",
    }

    def test_create_udf_instance(self):
        expected_vals = self.MOCK_LUA_UDF_VALS

        udf = UDF(**expected_vals)

        for key, value in expected_vals.items():
            self.assertEqual(value, getattr(udf, key), f"Did not find expected value for {key} in the udf class.")

    def test_get_create_query_from_udf(self):
        expected_query = "CREATE FUNCTION mock_keyspace.lua_var_length_counter(var text) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE lua AS 'return #var'"
        udf = UDF(**self.MOCK_LUA_UDF_VALS)
        actual_query = udf.get_create_query(ks="mock_keyspace", create_or_replace=False)

        self.assertEqual(expected_query, actual_query)

    def test_get_create_or_replace_query_from_udf(self):
        expected_query = "CREATE OR REPLACE FUNCTION mock_keyspace.lua_var_length_counter(var text) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE lua AS 'return #var'"
        udf = UDF(**self.MOCK_LUA_UDF_VALS)
        actual_query = udf.get_create_query(ks="mock_keyspace")

        self.assertEqual(expected_query, actual_query)

    def test_creating_udf_with_missing_required_argument(self):
        required_arg_names = ["name", "args", "called_on_null_input_returns", "return_type", "script"]

        for arg_name in required_arg_names:
            udf_args = self.MOCK_LUA_UDF_VALS.copy()
            udf_args.update({arg_name: None})

            with self.assertRaises(
                ValidationError, msg=f"Creating a udf without providing {arg_name} did not raise a ValidationError."
            ):
                UDF(**udf_args)

    def test_creating_udf_class_with_invalid_language(self):
        udf_vals = self.MOCK_LUA_UDF_VALS.copy()
        udf_vals.update({"language": "Java"})

        with self.assertRaises(
            ValidationError, msg="Creating UDF class with invalid language did not raise ValidationError."
        ):
            UDF(**udf_vals)

    def test_loading_udfs_with_lua_scripts(self):
        expected_vals = self.MOCK_LUA_UDF_VALS.copy()

        udf_yaml_filename = "./sdcm/utils/udf_scripts/lua_var_length_counter.yaml"
        udf = UDF.from_yaml(udf_yaml_filename)

        self.assertIsNotNone(udf)

        for key, value in expected_vals.items():
            self.assertEqual(value, getattr(udf, key), f"Did not find expected value for {key} in the udf class.")

    def test_loading_udfs_with_wasm_scripts(self):
        expected_vals = self.MOCK_WASM_UDF_VALS.copy()

        udf_yaml_filename = "./sdcm/utils/udf_scripts/wasm_plus.yaml"
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
