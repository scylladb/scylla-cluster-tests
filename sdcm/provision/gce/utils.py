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
# Copyright (c) 2026 ScyllaDB

"""
Utility functions for GCE provisioning.
"""

import re
import logging
from typing import Dict

from sdcm.provision.gce.constants import (
    MAX_LABEL_KEY_LENGTH,
    MAX_LABEL_VALUE_LENGTH,
)

LOGGER = logging.getLogger(__name__)


def normalize_gce_label_key(key: str) -> str:
    """
    Normalize a tag key to be valid for GCE labels.

    GCE label keys must:
    - Start with a lowercase letter
    - Contain only lowercase letters, numbers, underscores, and hyphens
    - Be 63 characters or less

    Args:
        key: The key to normalize

    Returns:
        Normalized key that complies with GCE label requirements
    """
    # Convert to lowercase
    normalized = key.lower()

    # Replace invalid characters with underscores
    normalized = re.sub(r"[^a-z0-9_-]", "_", normalized)

    # Ensure it starts with a letter
    if normalized and not normalized[0].isalpha():
        normalized = "a" + normalized

    # Truncate to maximum length
    if len(normalized) > MAX_LABEL_KEY_LENGTH:
        normalized = normalized[:MAX_LABEL_KEY_LENGTH]

    # Ensure it doesn't end with a hyphen or underscore
    normalized = normalized.rstrip("-_")

    return normalized


def normalize_gce_label_value(value: str) -> str:
    """
    Normalize a tag value to be valid for GCE labels.

    GCE label values must:
    - Contain only lowercase letters, numbers, underscores, and hyphens
    - Be 63 characters or less
    - Can be empty

    Args:
        value: The value to normalize

    Returns:
        Normalized value that complies with GCE label requirements
    """
    # Convert to lowercase
    normalized = value.lower()

    # Replace invalid characters with underscores
    normalized = re.sub(r"[^a-z0-9_-]", "_", normalized)

    # Truncate to maximum length
    if len(normalized) > MAX_LABEL_VALUE_LENGTH:
        normalized = normalized[:MAX_LABEL_VALUE_LENGTH]

    # Ensure it doesn't end with a hyphen or underscore
    normalized = normalized.rstrip("-_")

    return normalized


def tags_to_gce_labels(tags: Dict[str, str]) -> Dict[str, str]:
    """
    Convert a dictionary of tags to GCE-compliant labels.

    Args:
        tags: Dictionary of tags to convert

    Returns:
        Dictionary of GCE-compliant labels
    """
    labels = {}
    for key, value in tags.items():
        normalized_key = normalize_gce_label_key(key)
        normalized_value = normalize_gce_label_value(str(value))

        if normalized_key != key or normalized_value != str(value):
            LOGGER.debug("Normalized tag '%s: %s' to label '%s: %s'", key, value, normalized_key, normalized_value)

        labels[normalized_key] = normalized_value

    return labels


def validate_instance_name(name: str) -> bool:
    """
    Validate that an instance name meets GCE requirements.

    GCE instance names must:
    - Start with a lowercase letter
    - Contain only lowercase letters, numbers, and hyphens
    - Be 63 characters or less
    - Not end with a hyphen

    Args:
        name: Instance name to validate

    Returns:
        True if valid, False otherwise
    """
    if not name:
        return False

    if len(name) > 63:
        return False

    # Must start with a letter
    if not name[0].isalpha() or name[0].isupper():
        return False

    # Must not end with a hyphen
    if name.endswith("-"):
        return False

    # Must contain only lowercase letters, numbers, and hyphens
    if not re.match(r"^[a-z][a-z0-9-]*$", name):
        return False

    return True


def normalize_instance_name(name: str) -> str:
    """
    Normalize an instance name to meet GCE requirements.

    Args:
        name: Name to normalize

    Returns:
        Normalized name that meets GCE requirements
    """
    # Convert to lowercase
    normalized = name.lower()

    # Replace invalid characters with hyphens
    normalized = re.sub(r"[^a-z0-9-]", "-", normalized)

    # Ensure it starts with a letter
    if normalized and not normalized[0].isalpha():
        normalized = "n" + normalized

    # Truncate to 63 characters
    if len(normalized) > 63:
        normalized = normalized[:63]

    # Ensure it doesn't end with a hyphen
    normalized = normalized.rstrip("-")

    return normalized
