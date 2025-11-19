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

import tempfile
from pathlib import Path
from unittest import TestCase

import yaml

from sdcm.utils.uda import UDA, UDAS
from sdcm.utils.udf import UDF, UDFS


class TestUDA(TestCase):
    UDFS = {
        "accumulator": UDF(
            name="my_acc",
            args="(acc tuple<int, int>, val int)",
            called_on_null_input_returns="NULL",
            return_type="tuple<int, int>",
            language="lua",
            script="return {acc[1] + val, acc[2] + 1}",
        ),
        "negator": UDF(
            name="my_negator",
            args="(acc bigint)",
            called_on_null_input_returns="NULL",
            return_type="bigint",
            language="lua",
            script="return -acc",
        ),
        "reducer": UDF(
            name="sum_reductor",
            args="(acc1 bigint, acc2 bigint)",
            called_on_null_input_returns="NULL",
            return_type="bigint",
            language="lua",
            script="return acc1 + acc2",
        ),
    }

    def test_create_uda_instance(self):
        new_uda = UDA(
            name="my_uda",
            args="int",
            return_type="int",
            accumulator_udf=self.UDFS["accumulator"],
            reduce_udf=self.UDFS["reducer"],
            final_udf=self.UDFS["negator"],
            initial_condition="(0, 0)",
        )

        self.assertIsInstance(new_uda, UDA)

    def test_get_create_query_string(self):
        expected_create_query_string = "CREATE AGGREGATE testing.my_uda(int) SFUNC my_acc STYPE tuple<int, int> REDUCEFUNC sum_reductor FINALFUNC my_negator INITCOND (0, 0);"

        new_uda = UDA(
            name="my_uda",
            args="int",
            return_type="int",
            accumulator_udf=self.UDFS["accumulator"],
            reduce_udf=self.UDFS["reducer"],
            final_udf=self.UDFS["negator"],
            initial_condition="(0, 0)",
        )

        actual_create_query_string = new_uda.get_create_query_string(ks="testing")
        self.assertEqual(actual_create_query_string, expected_create_query_string)

    def test_load_uda_from_yaml(self):
        expected_create_string = "CREATE AGGREGATE testing.my_uda(int) SFUNC wasm_plus STYPE int REDUCEFUNC wasm_plus FINALFUNC wasm_simple_return_int INITCOND (0, 0);"

        data = {
            "name": "my_uda",
            "args": "int",
            "return_type": "int",
            "accumulator_udf_name": "wasm_plus",
            "reduce_udf_name": "wasm_plus",
            "final_udf_name": "wasm_simple_return_int",
            "initial_condition": "(0, 0)",
        }

        with tempfile.TemporaryDirectory() as tempdir:
            temp_yaml_path = Path(tempdir) / "new.yaml"
            with temp_yaml_path.open(mode="w") as outfile:
                yaml.safe_dump(data=data, stream=outfile)

            uda = UDA.from_yaml(uda_yaml_file_path=str(temp_yaml_path))
            create_string = uda.get_create_query_string(ks="testing")
            self.assertIsInstance(uda, UDA)
            self.assertEqual(expected_create_string, create_string)
            self.assertEqual(uda.args, data["args"])
            self.assertEqual(uda.return_type, data["return_type"])
            self.assertIsInstance(uda.accumulator_udf, UDF)
            self.assertEqual(uda.accumulator_udf.name, data["accumulator_udf_name"])
            self.assertIsInstance(uda.reduce_udf, UDF)
            self.assertEqual(uda.reduce_udf.name, data["reduce_udf_name"])
            self.assertIsInstance(uda.final_udf, UDF)
            self.assertEqual(uda.final_udf.name, data["final_udf_name"])
            self.assertEqual(uda.initial_condition, data["initial_condition"])

    def test_load_all_udas(self):
        self.assertGreater(len(UDFS.keys()), 1, "UDF count was not greater than 1.")
        for uda in UDAS.values():
            self.assertTrue(uda.name)
            self.assertTrue(uda.args)
            self.assertTrue(uda.return_type)
            self.assertIsInstance(uda.accumulator_udf, UDF)
            self.assertIsInstance(uda.final_udf, UDF)
