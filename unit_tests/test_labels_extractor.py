#!/usr/bin/env python3
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

"""Unit tests for labels_extractor module."""

import pytest
from pathlib import Path
import yaml

from sdcm.utils.labels_extractor import LabelsExtractor


class TestLabelsExtractor:
    """Test suite for LabelsExtractor class."""

    def test_load_authorized_labels_list_format(self, tmpdir):
        """Test loading authorized labels from list format YAML."""
        labels_file = tmpdir.join("authorized_labels.yaml")
        labels_data = ['label1', 'label2', 'label3']
        labels_file.write(yaml.dump(labels_data))

        extractor = LabelsExtractor(Path(tmpdir))
        result = extractor.load_authorized_labels(Path(labels_file))

        assert result == ['label1', 'label2', 'label3']

    def test_load_authorized_labels_dict_format(self, tmpdir):
        """Test loading authorized labels from dict format YAML."""
        labels_file = tmpdir.join("authorized_labels.yaml")
        labels_data = {'labels': ['label1', 'label2', 'label3']}
        labels_file.write(yaml.dump(labels_data))

        extractor = LabelsExtractor(Path(tmpdir))
        result = extractor.load_authorized_labels(Path(labels_file))

        assert result == ['label1', 'label2', 'label3']

    def test_load_authorized_labels_file_not_found(self, tmpdir):
        """Test that FileNotFoundError is raised when file doesn't exist."""
        extractor = LabelsExtractor(Path(tmpdir))
        non_existent = Path(tmpdir) / "nonexistent.yaml"

        with pytest.raises(FileNotFoundError, match="Authorized labels file not found"):
            extractor.load_authorized_labels(non_existent)

    def test_load_authorized_labels_invalid_yaml(self, tmpdir):
        """Test that YAMLError is raised for invalid YAML."""
        labels_file = tmpdir.join("authorized_labels.yaml")
        labels_file.write("invalid: yaml: content: [unclosed")

        extractor = LabelsExtractor(Path(tmpdir))

        with pytest.raises(yaml.YAMLError, match="Failed to parse authorized labels file"):
            extractor.load_authorized_labels(Path(labels_file))

    def test_validate_labels_all_authorized(self, tmpdir):
        """Test validation passes when all labels are authorized."""
        extractor = LabelsExtractor(Path(tmpdir))

        scan_result = {
            'all_labels': ['label1', 'label2', 'label3'],
            'folder_definitions': {},
            'jenkinsfiles': {},
            'test_methods': {}
        }
        authorized_labels = ['label1', 'label2', 'label3', 'label4']

        result = extractor.validate_labels(scan_result, authorized_labels)

        assert result['valid'] is True
        assert result['unknown_labels'] == []
        assert result['violations'] == []

    def test_validate_labels_with_unknown_labels(self, tmpdir):
        """Test validation fails when unknown labels are found."""
        extractor = LabelsExtractor(Path(tmpdir))

        scan_result = {
            'all_labels': ['label1', 'unknown-label', 'label3'],
            'folder_definitions': {},
            'jenkinsfiles': {},
            'test_methods': {}
        }
        authorized_labels = ['label1', 'label2', 'label3']

        result = extractor.validate_labels(scan_result, authorized_labels)

        assert result['valid'] is False
        assert 'unknown-label' in result['unknown_labels']
        assert len(result['unknown_labels']) == 1

    def test_validate_labels_tracks_violations_in_folder_definitions(self, tmpdir):
        """Test that violations in folder definitions are tracked."""
        extractor = LabelsExtractor(Path(tmpdir))

        scan_result = {
            'all_labels': ['authorized', 'unauthorized'],
            'folder_definitions': {
                'test/_folder_definitions.yaml': {
                    'folder_labels': ['authorized', 'unauthorized'],
                    'overrides': {}
                }
            },
            'jenkinsfiles': {},
            'test_methods': {}
        }
        authorized_labels = ['authorized']

        result = extractor.validate_labels(scan_result, authorized_labels)

        assert result['valid'] is False
        assert len(result['violations']) == 1
        assert result['violations'][0]['type'] == 'folder_definition'
        assert result['violations'][0]['label'] == 'unauthorized'

    def test_validate_labels_tracks_violations_in_jenkinsfiles(self, tmpdir):
        """Test that violations in jenkinsfiles are tracked."""
        extractor = LabelsExtractor(Path(tmpdir))

        scan_result = {
            'all_labels': ['authorized', 'bad-label'],
            'folder_definitions': {},
            'jenkinsfiles': {
                'test.jenkinsfile': ['authorized', 'bad-label']
            },
            'test_methods': {}
        }
        authorized_labels = ['authorized']

        result = extractor.validate_labels(scan_result, authorized_labels)

        assert result['valid'] is False
        assert len(result['violations']) == 1
        assert result['violations'][0]['type'] == 'jenkinsfile'
        assert result['violations'][0]['label'] == 'bad-label'
        assert result['violations'][0]['location'] == 'jobDescription'

    def test_validate_labels_tracks_violations_in_test_methods(self, tmpdir):
        """Test that violations in test methods are tracked."""
        extractor = LabelsExtractor(Path(tmpdir))

        scan_result = {
            'all_labels': ['valid', 'invalid'],
            'folder_definitions': {},
            'jenkinsfiles': {},
            'test_methods': {
                'test_file.py': {
                    'test_method': ['valid', 'invalid']
                }
            }
        }
        authorized_labels = ['valid']

        result = extractor.validate_labels(scan_result, authorized_labels)

        assert result['valid'] is False
        assert len(result['violations']) == 1
        assert result['violations'][0]['type'] == 'test_method'
        assert result['violations'][0]['label'] == 'invalid'
        assert 'test_method' in result['violations'][0]['location']

    def test_validate_labels_multiple_violations(self, tmpdir):
        """Test validation with multiple violations across different sources."""
        extractor = LabelsExtractor(Path(tmpdir))

        scan_result = {
            'all_labels': ['good', 'bad1', 'bad2'],
            'folder_definitions': {
                'folder1/_folder_definitions.yaml': {
                    'folder_labels': ['bad1'],
                    'overrides': {
                        'job1': ['bad2']
                    }
                }
            },
            'jenkinsfiles': {
                'test.jenkinsfile': ['bad1']
            },
            'test_methods': {
                'test.py': {
                    'test_func': ['bad2']
                }
            }
        }
        authorized_labels = ['good']

        result = extractor.validate_labels(scan_result, authorized_labels)

        assert result['valid'] is False
        assert set(result['unknown_labels']) == {'bad1', 'bad2'}
        assert len(result['violations']) == 4  # One in each location


class TestLabelsExtractorIntegration:
    """Integration tests for LabelsExtractor with real repository."""

    @pytest.mark.integration
    def test_scan_and_validate_with_real_repo(self):
        """Test scanning and validating the actual repository."""
        root_path = Path(__file__).parent.parent.parent
        authorized_labels_path = root_path / "authorized_labels.yaml"

        # Skip if authorized_labels.yaml doesn't exist
        if not authorized_labels_path.exists():
            pytest.skip("authorized_labels.yaml not found in repository root")

        extractor = LabelsExtractor(root_path)

        # Load authorized labels
        authorized_labels = extractor.load_authorized_labels(authorized_labels_path)
        assert len(authorized_labels) > 0

        # Scan a limited subset to keep test fast
        test_path = root_path / "docs" / "examples"
        if test_path.exists():
            scan_result = extractor.scan_repository(test_path)

            # Validate
            validation_result = extractor.validate_labels(scan_result, authorized_labels)

            # Just check the structure is correct
            assert 'valid' in validation_result
            assert 'unknown_labels' in validation_result
            assert 'violations' in validation_result
            assert isinstance(validation_result['valid'], bool)
            assert isinstance(validation_result['unknown_labels'], list)
            assert isinstance(validation_result['violations'], list)
