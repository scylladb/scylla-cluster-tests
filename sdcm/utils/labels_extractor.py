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

"""
Utility module for extracting labels/tags from test definitions, folder definitions,
and test method docstrings.
"""

import re
import ast
from pathlib import Path
from typing import List, Dict, Any
import yaml


class LabelsExtractor:
    """Extract labels from various sources in SCT test repository."""

    def __init__(self, root_path: Path):
        """
        Initialize the labels extractor.

        Args:
            root_path: Root path of the SCT repository
        """
        self.root_path = Path(root_path)

    def extract_folder_labels(self, folder_def_path: Path) -> Dict[str, Any]:
        """
        Extract labels from a _folder_definitions.yaml file.

        Args:
            folder_def_path: Path to _folder_definitions.yaml file

        Returns:
            Dictionary containing:
            - folder_labels: list of labels for the folder
            - overrides: dict mapping job names to their labels
        """
        result = {
            "folder_labels": [],
            "overrides": {}
        }

        try:
            with open(folder_def_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)

            if not data:
                return result

            # Extract folder-level labels
            if "labels" in data and isinstance(data["labels"], list):
                result["folder_labels"] = data["labels"]

            # Extract override labels
            if "overrides" in data and isinstance(data["overrides"], dict):
                for job_name, job_data in data["overrides"].items():
                    if isinstance(job_data, dict) and "labels" in job_data:
                        if isinstance(job_data["labels"], list):
                            result["overrides"][job_name] = job_data["labels"]

        except (FileNotFoundError, yaml.YAMLError):
            pass

        return result

    def extract_jenkinsfile_labels(self, jenkinsfile_path: Path) -> List[str]:
        """
        Extract labels from jobDescription annotation in a .jenkinsfile.

        Supports formats:
        - Labels: label1, label2, label3
        - Labels:
            - label1
            - label2

        Args:
            jenkinsfile_path: Path to .jenkinsfile

        Returns:
            List of labels found in the jobDescription
        """
        labels = []

        try:
            with open(jenkinsfile_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Look for jobDescription block
            job_desc_pattern = r'/\*\*\s*jobDescription(.*?)\*/'
            match = re.search(job_desc_pattern, content, re.DOTALL)

            if match:
                description = match.group(1)
                labels = self._extract_labels_from_docstring(description)

        except FileNotFoundError:
            pass

        return labels

    def extract_test_method_labels(self, test_file_path: Path) -> Dict[str, List[str]]:
        """
        Extract labels from test method docstrings in Python test files.

        Args:
            test_file_path: Path to Python test file (*_test.py)

        Returns:
            Dictionary mapping test method names to their labels
        """
        result = {}

        try:
            with open(test_file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Parse the Python file
            try:
                tree = ast.parse(content)
            except SyntaxError:
                return result

            # Find all test methods
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    # Check if it's a test method (starts with 'test_')
                    if node.name.startswith('test_'):
                        docstring = ast.get_docstring(node)
                        if docstring:
                            labels = self._extract_labels_from_docstring(docstring)
                            if labels:
                                result[node.name] = labels

        except FileNotFoundError:
            pass

        return result

    def _extract_labels_from_docstring(self, docstring: str) -> List[str]:
        """
        Extract labels from a docstring.

        Supports formats:
        - Labels: label1, label2, label3
        - Labels: label1,
                  label2, label3
        - Labels:
            - label1
            - label2
        - Labels:
            - label1,
            - label2,
            - label3

        Args:
            docstring: The docstring text

        Returns:
            List of labels
        """
        labels = []

        # Try list format first (more specific):
        # Labels:
        #   - label1
        #   - label2
        # or with trailing commas:
        # Labels:
        #   - label1,
        #   - label2
        list_pattern = r'Labels:\s*\n((?:\s*-\s*[^\n]+\n?)+)'
        list_match = re.search(list_pattern, docstring, re.IGNORECASE)

        if list_match:
            labels_block = list_match.group(1)
            # Extract each label and remove trailing commas
            label_items = re.findall(r'-\s*([^\n]+)', labels_block)
            labels = [label.strip().rstrip(',').strip() for label in label_items if label.strip()]
            return labels

        # Try inline format: Labels: label1, label2, label3
        # This also handles multi-line inline format:
        # Labels: label1,
        #         label2, label3
        # Strategy: Find "Labels:" with content on the same line, then capture continuation lines
        # that are indented
        # Pattern explanation:
        # - Labels:[ \t]+ - Match "Labels:" followed by at least one space/tab (not newline)
        # - ([^\n]+) - Capture at least one character on the same line (not a newline)
        # - (?:\n\s+[^\n]+)* - Then optionally match indented continuation lines
        # - (?=...) - Stop when we hit blank line, non-indented line, closing comment, or end
        inline_pattern = r'Labels:[ \t]+([^\n]+(?:\n\s+[^\n]+)*?)(?=\n\s*\n|\n\S|\*\/|$)'
        inline_match = re.search(inline_pattern, docstring, re.IGNORECASE)

        if inline_match:
            labels_str = inline_match.group(1).strip()
            # Make sure it's not empty and doesn't start with a dash (which would be list format)
            if labels_str and not labels_str.startswith('-'):
                # Replace newlines and extra whitespace with a single space
                labels_str = re.sub(r'\s+', ' ', labels_str)
                # Split by commas and filter out empty strings
                labels = [label.strip() for label in labels_str.split(',') if label.strip()]
                if labels:
                    return labels

        return labels

    def scan_repository(self, path: Path = None) -> Dict[str, Any]:
        """
        Scan the entire repository (or specified path) for all labels.

        Args:
            path: Specific path to scan, defaults to root_path

        Returns:
            Dictionary with:
            - folder_definitions: dict of folder paths to their labels
            - jenkinsfiles: dict of jenkinsfile paths to their labels
            - test_methods: dict of test file paths to dict of methods and labels
            - all_labels: sorted list of all unique labels found
        """
        scan_path = path or self.root_path
        result = {
            "folder_definitions": {},
            "jenkinsfiles": {},
            "test_methods": {},
            "all_labels": set()
        }

        # Find all _folder_definitions.yaml files
        for folder_def in scan_path.rglob("_folder_definitions.yaml"):
            labels_data = self.extract_folder_labels(folder_def)
            relative_path = str(folder_def.relative_to(self.root_path))
            result["folder_definitions"][relative_path] = labels_data

            # Add to all_labels
            result["all_labels"].update(labels_data["folder_labels"])
            for override_labels in labels_data["overrides"].values():
                result["all_labels"].update(override_labels)

        # Find all .jenkinsfile files
        for jenkinsfile in scan_path.rglob("*.jenkinsfile"):
            labels = self.extract_jenkinsfile_labels(jenkinsfile)
            if labels:
                relative_path = str(jenkinsfile.relative_to(self.root_path))
                result["jenkinsfiles"][relative_path] = labels
                result["all_labels"].update(labels)

        # Find all Python test files
        for test_file in scan_path.glob("*_test.py"):
            method_labels = self.extract_test_method_labels(test_file)
            if method_labels:
                relative_path = str(test_file.relative_to(self.root_path))
                result["test_methods"][relative_path] = method_labels

                # Add to all_labels
                for labels in method_labels.values():
                    result["all_labels"].update(labels)

        # Convert set to sorted list for consistent output
        result["all_labels"] = sorted(result["all_labels"])

        return result

    def get_labels_statistics(self, scan_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate statistics from scan results.

        Args:
            scan_result: Result from scan_repository()

        Returns:
            Dictionary with statistics
        """
        stats = {
            "total_unique_labels": len(scan_result["all_labels"]),
            "total_folder_definitions": len(scan_result["folder_definitions"]),
            "total_jenkinsfiles_with_labels": len(scan_result["jenkinsfiles"]),
            "total_test_files_with_labels": len(scan_result["test_methods"]),
            "total_test_methods_with_labels": sum(
                len(methods) for methods in scan_result["test_methods"].values()
            ),
            "label_usage": {}
        }

        # Count usage of each label
        label_counts = {}
        for labels_data in scan_result["folder_definitions"].values():
            for label in labels_data["folder_labels"]:
                label_counts[label] = label_counts.get(label, 0) + 1
            for override_labels in labels_data["overrides"].values():
                for label in override_labels:
                    label_counts[label] = label_counts.get(label, 0) + 1

        for labels in scan_result["jenkinsfiles"].values():
            for label in labels:
                label_counts[label] = label_counts.get(label, 0) + 1

        for methods in scan_result["test_methods"].values():
            for labels in methods.values():
                for label in labels:
                    label_counts[label] = label_counts.get(label, 0) + 1

        stats["label_usage"] = dict(sorted(label_counts.items(), key=lambda x: x[1], reverse=True))

        return stats

    def load_authorized_labels(self, authorized_labels_path: Path) -> List[str]:
        """
        Load authorized labels from a YAML file.

        Args:
            authorized_labels_path: Path to authorized_labels.yaml file

        Returns:
            List of authorized label strings

        Raises:
            FileNotFoundError: If the authorized labels file doesn't exist
            yaml.YAMLError: If the YAML file is malformed
        """
        try:
            with open(authorized_labels_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)

            if not data:
                return []

            # Support both list format and dict with 'labels' key
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and 'labels' in data:
                if isinstance(data['labels'], list):
                    return data['labels']

            return []

        except FileNotFoundError as exc:
            raise FileNotFoundError(
                f"Authorized labels file not found: {authorized_labels_path}"
            ) from exc
        except yaml.YAMLError as exc:
            raise yaml.YAMLError(
                f"Failed to parse authorized labels file: {authorized_labels_path}"
            ) from exc

    def validate_labels(
        self,
        scan_result: Dict[str, Any],
        authorized_labels: List[str]
    ) -> Dict[str, Any]:
        """
        Validate that all labels in scan results are authorized.

        Args:
            scan_result: Result from scan_repository()
            authorized_labels: List of authorized label strings

        Returns:
            Dictionary containing:
            - valid: bool, True if all labels are authorized
            - unknown_labels: list of labels not in authorized list
            - violations: list of dicts with details about where unknown labels are used
        """
        authorized_set = set(authorized_labels)
        found_labels = set(scan_result["all_labels"])
        unknown_labels = found_labels - authorized_set

        result = {
            "valid": len(unknown_labels) == 0,
            "unknown_labels": sorted(unknown_labels),
            "violations": []  # type: ignore
        }

        if not unknown_labels:
            return result

        # Find all locations where unknown labels are used
        # Check folder definitions
        for path_str, labels_data in scan_result["folder_definitions"].items():
            for label in labels_data["folder_labels"]:
                if label in unknown_labels:
                    result["violations"].append({
                        "type": "folder_definition",
                        "file": path_str,
                        "location": "folder labels",
                        "label": label
                    })

            for job_name, override_labels in labels_data["overrides"].items():
                for label in override_labels:
                    if label in unknown_labels:
                        result["violations"].append({
                            "type": "folder_definition",
                            "file": path_str,
                            "location": f"override: {job_name}",
                            "label": label
                        })

        # Check jenkinsfiles
        for path_str, labels in scan_result["jenkinsfiles"].items():
            for label in labels:
                if label in unknown_labels:
                    result["violations"].append({
                        "type": "jenkinsfile",
                        "file": path_str,
                        "location": "jobDescription",
                        "label": label
                    })

        # Check test methods
        for path_str, methods in scan_result["test_methods"].items():
            for method_name, labels in methods.items():
                for label in labels:
                    if label in unknown_labels:
                        result["violations"].append({
                            "type": "test_method",
                            "file": path_str,
                            "location": f"method: {method_name}",
                            "label": label
                        })

        return result
