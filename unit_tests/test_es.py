"""Unit tests for Elasticsearch integration."""

import unittest
from unittest.mock import patch

from sdcm.es import sanitize_elasticsearch_data, ES


class TestSanitizeElasticsearchData(unittest.TestCase):
    """Tests for sanitize_elasticsearch_data function."""

    def test_sanitize_nan_value(self):
        """Test that NaN values are replaced with None."""
        data = {"value": float("nan")}
        result = sanitize_elasticsearch_data(data)
        self.assertIsNone(result["value"])

    def test_sanitize_inf_value(self):
        """Test that Infinity values are replaced with None."""
        data = {"value": float("inf")}
        result = sanitize_elasticsearch_data(data)
        self.assertIsNone(result["value"])

    def test_sanitize_neg_inf_value(self):
        """Test that negative Infinity values are replaced with None."""
        data = {"value": float("-inf")}
        result = sanitize_elasticsearch_data(data)
        self.assertIsNone(result["value"])

    def test_sanitize_normal_float(self):
        """Test that normal float values are preserved."""
        data = {"value": 3.14159}
        result = sanitize_elasticsearch_data(data)
        self.assertEqual(result["value"], 3.14159)

    def test_sanitize_nested_dict(self):
        """Test that nested dictionaries are sanitized recursively."""
        data = {
            "outer": {
                "inner": {
                    "nan_value": float("nan"),
                    "normal_value": 42.0,
                }
            }
        }
        result = sanitize_elasticsearch_data(data)
        self.assertIsNone(result["outer"]["inner"]["nan_value"])
        self.assertEqual(result["outer"]["inner"]["normal_value"], 42.0)

    def test_sanitize_list(self):
        """Test that lists are sanitized recursively."""
        data = {"values": [1.0, float("nan"), 3.0, float("inf")]}
        result = sanitize_elasticsearch_data(data)
        self.assertEqual(result["values"], [1.0, None, 3.0, None])

    def test_sanitize_mixed_types(self):
        """Test that mixed data types are handled correctly."""
        data = {
            "string": "test",
            "int": 42,
            "float": 3.14,
            "nan": float("nan"),
            "inf": float("inf"),
            "bool": True,
            "none": None,
            "list": [1, float("nan"), "text"],
            "nested": {
                "inner_nan": float("nan"),
                "inner_value": 100,
            },
        }
        result = sanitize_elasticsearch_data(data)

        self.assertEqual(result["string"], "test")
        self.assertEqual(result["int"], 42)
        self.assertEqual(result["float"], 3.14)
        self.assertIsNone(result["nan"])
        self.assertIsNone(result["inf"])
        self.assertTrue(result["bool"])
        self.assertIsNone(result["none"])
        self.assertEqual(result["list"], [1, None, "text"])
        self.assertIsNone(result["nested"]["inner_nan"])
        self.assertEqual(result["nested"]["inner_value"], 100)

    def test_sanitize_complex_nested_structure(self):
        """Test sanitization of complex nested structures."""
        data = {
            "results": {
                "stats": [
                    {"max": 100.0, "min": float("nan"), "avg": float("inf")},
                    {"max": 200.0, "min": 50.0, "avg": 125.0},
                ],
                "histograms": {
                    "latency": [
                        {"p99": float("nan"), "p95": 1.5},
                        {"p99": 2.0, "p95": float("inf")},
                    ]
                },
            }
        }
        result = sanitize_elasticsearch_data(data)

        self.assertEqual(result["results"]["stats"][0]["max"], 100.0)
        self.assertIsNone(result["results"]["stats"][0]["min"])
        self.assertIsNone(result["results"]["stats"][0]["avg"])
        self.assertEqual(result["results"]["stats"][1]["max"], 200.0)
        self.assertEqual(result["results"]["stats"][1]["min"], 50.0)
        self.assertEqual(result["results"]["stats"][1]["avg"], 125.0)

        self.assertIsNone(result["results"]["histograms"]["latency"][0]["p99"])
        self.assertEqual(result["results"]["histograms"]["latency"][0]["p95"], 1.5)
        self.assertEqual(result["results"]["histograms"]["latency"][1]["p99"], 2.0)
        self.assertIsNone(result["results"]["histograms"]["latency"][1]["p95"])

    def test_sanitize_empty_structures(self):
        """Test that empty structures are handled correctly."""
        # Empty dict
        result = sanitize_elasticsearch_data({})
        self.assertEqual(result, {})

        # Empty list
        result = sanitize_elasticsearch_data([])
        self.assertEqual(result, [])

        # Dict with empty nested structures
        data = {"empty_dict": {}, "empty_list": []}
        result = sanitize_elasticsearch_data(data)
        self.assertEqual(result["empty_dict"], {})
        self.assertEqual(result["empty_list"], [])


class TestESClass(unittest.TestCase):
    """Tests for ES class methods."""

    @patch("sdcm.es.KeyStore")
    def test_update_doc_sanitizes_data(self, mock_keystore):
        """Test that update_doc sanitizes data before sending to Elasticsearch."""
        # Mock KeyStore to avoid authentication issues
        mock_keystore.return_value.get_elasticsearch_token.return_value = {"hosts": ["http://localhost:9200"]}

        # Create ES instance and mock the update method
        with patch.object(ES, "update") as mock_update:
            es = ES()

            # Data with NaN and Inf values
            body = {
                "value": float("nan"),
                "nested": {"inf_value": float("inf")},
            }

            es.update_doc("test_index", "test_id", body)

            # Verify update was called with sanitized data
            mock_update.assert_called_once()
            call_args = mock_update.call_args
            sanitized_body = call_args[1]["body"]["doc"]

            self.assertIsNone(sanitized_body["value"])
            self.assertIsNone(sanitized_body["nested"]["inf_value"])

    @patch("sdcm.es.KeyStore")
    def test_create_doc_sanitizes_data(self, mock_keystore):
        """Test that create_doc sanitizes data before sending to Elasticsearch."""
        # Mock KeyStore to avoid authentication issues
        mock_keystore.return_value.get_elasticsearch_token.return_value = {"hosts": ["http://localhost:9200"]}

        # Create ES instance and mock methods
        with (
            patch.object(ES, "exists", return_value=False),
            patch.object(ES, "create") as mock_create,
            patch.object(ES, "_create_index"),
        ):
            es = ES()

            # Data with NaN and Inf values
            body = {"stats": {"max": 100.0, "min": float("nan"), "avg": float("inf")}}

            es.create_doc("test_index", "test_id", body)

            # Verify create was called with sanitized data
            mock_create.assert_called_once()
            call_args = mock_create.call_args
            sanitized_body = call_args[1]["body"]

            self.assertEqual(sanitized_body["stats"]["max"], 100.0)
            self.assertIsNone(sanitized_body["stats"]["min"])
            self.assertIsNone(sanitized_body["stats"]["avg"])


if __name__ == "__main__":
    unittest.main()
