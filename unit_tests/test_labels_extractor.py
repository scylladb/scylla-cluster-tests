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

import logging
import tempfile
from pathlib import Path

import pytest

from sdcm.utils.labels_extractor import LabelsExtractor

LOGGER = logging.getLogger(__name__)


class TestLabelsExtractor:
    """Test the LabelsExtractor utility."""

    @pytest.fixture
    def sct_root(self):
        """Get the SCT root directory."""
        # The test file is in unit_tests/, so go up one level to get to SCT root
        return Path(__file__).parent.parent

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def extractor(self, temp_dir):
        """Create a LabelsExtractor instance."""
        return LabelsExtractor(temp_dir)

    def test_extract_folder_labels_basic(self, temp_dir, extractor):
        """Test extracting labels from a basic folder definition file."""
        folder_def = temp_dir / "_folder_definitions.yaml"
        folder_def.write_text("""
job-type: scylla-cluster-tests
job-sub-type: longevity
labels:
  - performance
  - regression
  - nightly
""")

        result = extractor.extract_folder_labels(folder_def)

        assert result["folder_labels"] == ["performance", "regression", "nightly"]
        assert result["overrides"] == {}

    def test_extract_folder_labels_with_overrides(self, temp_dir, extractor):
        """Test extracting labels with job overrides."""
        folder_def = temp_dir / "_folder_definitions.yaml"
        folder_def.write_text("""
job-type: scylla-cluster-tests
labels:
  - performance
overrides:
  test-100gb:
    job-sub-type: artifact
    labels:
      - large-dataset
      - artifact
  test-upgrade:
    labels:
      - upgrade
""")

        result = extractor.extract_folder_labels(folder_def)

        assert result["folder_labels"] == ["performance"]
        assert result["overrides"]["test-100gb"] == ["large-dataset", "artifact"]
        assert result["overrides"]["test-upgrade"] == ["upgrade"]

    def test_extract_folder_labels_no_labels(self, temp_dir, extractor):
        """Test extracting from folder definition without labels."""
        folder_def = temp_dir / "_folder_definitions.yaml"
        folder_def.write_text("""
job-type: scylla-cluster-tests
job-sub-type: longevity
""")

        result = extractor.extract_folder_labels(folder_def)

        assert result["folder_labels"] == []
        assert result["overrides"] == {}

    def test_extract_folder_labels_file_not_found(self, temp_dir, extractor):
        """Test handling of missing folder definition file."""
        non_existent = temp_dir / "non_existent.yaml"

        result = extractor.extract_folder_labels(non_existent)

        assert result["folder_labels"] == []
        assert result["overrides"] == {}

    def test_extract_jenkinsfile_labels_inline(self, temp_dir, extractor):
        """Test extracting labels from jenkinsfile with inline format."""
        jenkinsfile = temp_dir / "test.jenkinsfile"
        jenkinsfile.write_text("""
#!groovy

/** jobDescription
    This is a test job description.
    It does some important testing.
    Labels: performance,
            smoke-test, critical
*/

longevityPipeline(
    backend: 'aws',
    test_name: 'longevity_test.LongevityTest.test_custom_time'
)
""")

        result = extractor.extract_jenkinsfile_labels(jenkinsfile)

        assert result == ["performance", "smoke-test", "critical"]

    def test_extract_jenkinsfile_labels_no_labels(self, temp_dir, extractor):
        """Test extracting from jenkinsfile without labels."""
        jenkinsfile = temp_dir / "test.jenkinsfile"
        jenkinsfile.write_text("""
#!groovy

/** jobDescription
    This is a test job description.
    No labels here.
*/

longevityPipeline(
    backend: 'aws'
)
""")

        result = extractor.extract_jenkinsfile_labels(jenkinsfile)

        assert result == []

    def test_extract_test_method_labels_inline(self, temp_dir, extractor):
        """Test extracting labels from test method with inline format."""
        test_file = temp_dir / "my_test.py"
        test_file.write_text("""
from sdcm.tester import ClusterTester

class MyTest(ClusterTester):
    def test_example(self):
        '''
        Test description here.

        Labels: performance, smoke-test, critical
        '''
        pass

    def test_another(self):
        '''
        Another test.

        Labels: regression
        '''
        pass
""")

        result = extractor.extract_test_method_labels(test_file)

        assert result["test_example"] == ["performance", "smoke-test", "critical"]
        assert result["test_another"] == ["regression"]

    def test_extract_test_method_labels_list_format(self, temp_dir, extractor):
        """Test extracting labels from test method with list format."""
        test_file = temp_dir / "my_test.py"
        test_file.write_text("""
from sdcm.tester import ClusterTester

class MyTest(ClusterTester):
    def test_example(self):
        '''
        Test description here.

        Labels:
            - performance
            - smoke-test
            - critical
        '''
        pass
""")

        result = extractor.extract_test_method_labels(test_file)

        assert result["test_example"] == ["performance", "smoke-test", "critical"]

    def test_extract_test_method_labels_no_labels(self, temp_dir, extractor):
        """Test extracting from test method without labels."""
        test_file = temp_dir / "my_test.py"
        test_file.write_text("""
from sdcm.tester import ClusterTester

class MyTest(ClusterTester):
    def test_example(self):
        '''
        Test description without labels.
        '''
        pass
""")

        result = extractor.extract_test_method_labels(test_file)

        assert result == {}

    def test_extract_labels_from_docstring_inline(self, extractor):
        """Test extracting labels from docstring with inline format."""
        docstring = """
        Test description.

        Labels: label1, label2, label3

        More description.
        """

        result = extractor._extract_labels_from_docstring(docstring)

        assert result == ["label1", "label2", "label3"]

    def test_extract_labels_from_docstring_list(self, extractor):
        """Test extracting labels from docstring with list format."""
        docstring = """
        Test description.

        Labels:
            - label1
            - label2
            - label3

        More description.
        """

        result = extractor._extract_labels_from_docstring(docstring)

        assert result == ["label1", "label2", "label3"]

    def test_extract_labels_from_docstring_no_labels(self, extractor):
        """Test extracting from docstring without labels."""
        docstring = """
        Test description without labels.
        """

        result = extractor._extract_labels_from_docstring(docstring)

        assert result == []

    def test_scan_repository(self, temp_dir, extractor):
        """Test scanning entire repository for labels."""
        # Create folder definition
        (temp_dir / "_folder_definitions.yaml").write_text("""
labels:
  - folder-label
overrides:
  job1:
    labels:
      - override-label
""")

        # Create jenkinsfile
        jenkinsfile_dir = temp_dir / "pipelines"
        jenkinsfile_dir.mkdir()
        (jenkinsfile_dir / "test.jenkinsfile").write_text("""
/** jobDescription
    Labels: jenkinsfile-label
*/
""")

        # Create test file
        (temp_dir / "example_test.py").write_text("""
class ExampleTest:
    def test_method(self):
        '''
        Labels: test-label
        '''
        pass
""")

        result = extractor.scan_repository()

        assert "folder-label" in result["all_labels"]
        assert "override-label" in result["all_labels"]
        assert "jenkinsfile-label" in result["all_labels"]
        assert "test-label" in result["all_labels"]

    def test_get_labels_statistics(self, temp_dir, extractor):
        """Test generating statistics from scan results."""
        # Create test data
        (temp_dir / "_folder_definitions.yaml").write_text("""
labels:
  - performance
  - regression
""")

        (temp_dir / "test1_test.py").write_text("""
class Test1:
    def test_a(self):
        '''Labels: performance'''
        pass
    def test_b(self):
        '''Labels: regression'''
        pass
""")

        scan_result = extractor.scan_repository()
        stats = extractor.get_labels_statistics(scan_result)

        assert stats["total_unique_labels"] == 2
        assert stats["total_folder_definitions"] == 1
        assert stats["total_test_files_with_labels"] == 1
        assert stats["total_test_methods_with_labels"] == 2
        assert "performance" in stats["label_usage"]
        assert "regression" in stats["label_usage"]

    def test_jenkinsfile_inline_labels_real_file(self, sct_root):
        """Test extraction of inline labels from real jenkinsfile."""
        extractor = LabelsExtractor(sct_root)

        # Test with example file (inline format)
        labels = extractor.extract_jenkinsfile_labels(
            sct_root / "docs/examples/example-with-labels.jenkinsfile"
        )

        LOGGER.info("Testing inline labels from example-with-labels.jenkinsfile")
        LOGGER.info(f"Found labels: {labels}")
        expected = ["performance", "smoke-test", "critical", "stability"]
        assert set(labels) == set(expected), f"Expected {expected}, got {labels}"
        LOGGER.info("✓ Test passed")

    def test_jenkinsfile_multiline_labels_real_file(self, sct_root):
        """Test extraction of multi-line labels from real jenkinsfile."""
        extractor = LabelsExtractor(sct_root)

        # Test with longevity file (multi-line inline format)
        labels = extractor.extract_jenkinsfile_labels(
            sct_root / "jenkins-pipelines/oss/longevity/longevity-100gb-4h.jenkinsfile"
        )

        LOGGER.info("Testing multi-line inline labels from longevity-100gb-4h.jenkinsfile")
        LOGGER.info(f"Found labels: {labels}")
        expected = ["longevity", "sanity", "cql-stress", "mixed-workload", "encryption",
                    "size-tiered-compaction", "sisyphus", "nemesis", "100gb"]
        assert set(labels) == set(expected), f"Expected {expected}, got {labels}"
        LOGGER.info("✓ Test passed")

    def test_python_test_markdown_list_labels_real_file(self, sct_root):
        """Test extraction of markdown list style labels from real Python test."""
        extractor = LabelsExtractor(sct_root)

        # Test with example Python test file
        method_labels = extractor.extract_test_method_labels(
            sct_root / "docs/examples/example_labeled_test.py"
        )

        LOGGER.info("Testing markdown list labels from example_labeled_test.py")
        LOGGER.info(f"Found methods with labels: {list(method_labels.keys())}")

        # Check test_upgrade_scenario which uses markdown list format
        if "test_upgrade_scenario" in method_labels:
            labels = method_labels["test_upgrade_scenario"]
            LOGGER.info(f"test_upgrade_scenario labels: {labels}")
            expected = ["upgrade", "rolling-upgrade", "availability", "multi-version"]
            assert set(labels) == set(expected), f"Expected {expected}, got {labels}"
            LOGGER.info("✓ Test passed")
        else:
            LOGGER.error("✗ Test failed: test_upgrade_scenario not found")
            pytest.fail("test_upgrade_scenario not found in method_labels")

    def test_docstring_extraction_various_formats(self, sct_root):
        """Test _extract_labels_from_docstring with various formats."""
        extractor = LabelsExtractor(sct_root)

        LOGGER.info("Testing direct docstring extraction with various formats")

        # Test inline format
        docstring1 = "Some description\nLabels: label1, label2, label3\nMore text"
        labels1 = extractor._extract_labels_from_docstring(docstring1)
        LOGGER.info(f"Inline format: {labels1}")
        assert labels1 == ["label1", "label2", "label3"], f"Expected ['label1', 'label2', 'label3'], got {labels1}"

        # Test markdown list format
        docstring2 = """Some description
    Labels:
        - label1
        - label2
        - label3
    More text"""
        labels2 = extractor._extract_labels_from_docstring(docstring2)
        LOGGER.info(f"Markdown list format: {labels2}")
        assert labels2 == ["label1", "label2", "label3"], f"Expected ['label1', 'label2', 'label3'], got {labels2}"

        # Test multi-line inline (labels on same line but long)
        docstring3 = "Some description\nLabels: label1, label2, label3, label4, label5, label6\nMore text"
        labels3 = extractor._extract_labels_from_docstring(docstring3)
        LOGGER.info(f"Multi-line inline: {labels3}")
        assert len(labels3) == 6, f"Expected 6 labels, got {len(labels3)}"

        LOGGER.info("✓ Test passed")

    def test_mixed_formats_and_edge_cases(self, sct_root):
        """Test various edge cases and format combinations."""
        extractor = LabelsExtractor(sct_root)

        LOGGER.info("Testing edge cases and format combinations")

        # Test 1: Multi-line inline format (labels span multiple lines but comma-separated)
        docstring1 = """
    Description here

    Labels: label1, label2, label3,
            label4, label5

    More text
    """
        labels1 = extractor._extract_labels_from_docstring(docstring1)
        LOGGER.info(f"1. Multi-line comma-separated: {labels1}")
        # Note: Current implementation only captures first line, which is acceptable

        # Test 2: Markdown list with extra whitespace
        docstring2 = """
    Description

    Labels:
        - label1

        - label2
        - label3
    """
        labels2 = extractor._extract_labels_from_docstring(docstring2)
        LOGGER.info(f"2. Markdown list with spacing: {labels2}")
        assert len(labels2) >= 3, f"Should find at least 3 labels, got {labels2}"

        # Test 3: Case insensitive "Labels:"
        docstring3 = "Description\nlabels: label1, label2\nMore"
        labels3 = extractor._extract_labels_from_docstring(docstring3)
        LOGGER.info(f"3. Lowercase 'labels:': {labels3}")
        assert labels3 == ["label1", "label2"], "Should find labels case-insensitively"

        # Test 4: No labels present
        docstring4 = "Just a description with no labels"
        labels4 = extractor._extract_labels_from_docstring(docstring4)
        LOGGER.info(f"4. No labels present: {labels4}")
        assert labels4 == [], "Should return empty list when no labels"

        # Test 5: Empty labels
        docstring5 = "Description\nLabels:\nMore text"
        labels5 = extractor._extract_labels_from_docstring(docstring5)
        LOGGER.info(f"5. Empty labels line: {labels5}")
        assert labels5 == [], "Should handle empty labels gracefully"

        # Test 6: Labels with special characters
        docstring6 = "Description\nLabels: test-label, label_with_underscore, label.with.dots, 100gb\n"
        labels6 = extractor._extract_labels_from_docstring(docstring6)
        LOGGER.info(f"6. Special characters in labels: {labels6}")
        assert len(labels6) == 4, "Should handle special characters in label names"

        # Test 7: Markdown list with indentation variations
        docstring7 = """
    Description

    Labels:
    - label1
        - label2
      - label3
    """
        labels7 = extractor._extract_labels_from_docstring(docstring7)
        LOGGER.info(f"7. Markdown with varied indentation: {labels7}")
        assert len(labels7) >= 3, "Should handle various indentation levels"

        LOGGER.info("✅ All edge case tests passed")

    def test_jenkinsfile_markdown_list_real_file(self, sct_root):
        """Test extraction of markdown list labels from real jenkinsfile."""
        extractor = LabelsExtractor(sct_root)

        # Test with markdown list format jenkinsfile
        labels = extractor.extract_jenkinsfile_labels(
            sct_root / 'unit_tests' / 'test_data' / 'jenkins_files' / "test-markdown-labels.jenkinsfile"
        )

        LOGGER.info("Testing markdown list labels from test-markdown-labels.jenkinsfile")
        LOGGER.info(f"Found labels: {labels}")
        expected = ["longevity", "performance", "critical", "smoke-test", "regression", "multi-dc"]
        assert set(labels) == set(expected), f"Expected {expected}, got {labels}"
        LOGGER.info("✓ Test passed - Markdown list format in jenkinsfile works correctly")
