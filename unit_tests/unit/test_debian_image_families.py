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

import re
from pathlib import Path

# SCT-943: GCP only publishes image families for currently-supported (non-EOL) Debian
# releases. When a Debian release goes EOL and GCP removes its `debian-cloud` image
# family, or when a new Debian release becomes the recommended target, update this set
# to match — this is the single place that encodes "which Debian releases SCT covers".
SUPPORTED_DEBIAN_IMAGE_FAMILIES = {"debian-12", "debian-13"}

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEBIAN_FAMILY_RE = re.compile(r"debian-cloud/global/images/family/(debian-\d+)")


def _iter_scanned_files():
    for pattern in ("test-cases/**/*.yaml", "jenkins-pipelines/**/*.jenkinsfile"):
        yield from PROJECT_ROOT.glob(pattern)


def test_only_supported_debian_image_families_referenced():
    """SCT-943: no test-case YAML or Jenkinsfile should reference a Debian GCE image
    family that is not in SUPPORTED_DEBIAN_IMAGE_FAMILIES (e.g. an EOL release GCP has
    removed, like debian-11).

    The scan is scoped to test-cases/ and jenkins-pipelines/ only (see
    _iter_scanned_files) — it never traverses unit_tests/, which contains an
    intentional, offline debian-10 fixture unrelated to any live GCP call.
    """
    violations = []
    for file_path in _iter_scanned_files():
        content = file_path.read_text(encoding="utf-8")
        for match in DEBIAN_FAMILY_RE.finditer(content):
            family = match.group(1)
            if family not in SUPPORTED_DEBIAN_IMAGE_FAMILIES:
                violations.append((file_path.relative_to(PROJECT_ROOT), family))

    assert not violations, "Unsupported/EOL Debian image family referenced in:\n" + "\n".join(
        f"  {path} -> {family}" for path, family in violations
    )
