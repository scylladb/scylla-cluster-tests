"""Guard against reintroducing GCP GCE image families that have been removed upstream.

GCP periodically deletes public image families once the underlying OS reaches end of
life (e.g. `debian-cloud/debian-11` and `ubuntu-os-cloud/ubuntu-2004-lts`). Any SCT
config or Jenkinsfile still pointing at such a family fails config validation with a
404 when the family lookup is attempted. This test walks the config/CI trees and makes
sure none of them reference a known-dead family string.
"""

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent.parent

SCAN_DIRS = (
    "test-cases",
    "configurations",
    "defaults",
    "jenkins-pipelines",
)

# GCE image family strings for OS versions GCP has removed from its public image projects.
EOL_GCE_IMAGE_FAMILIES = (
    pytest.param("debian-11", id="debian-11"),
    pytest.param("ubuntu-2004-lts", id="ubuntu-2004-lts"),
)


def _iter_text_files():
    for scan_dir in SCAN_DIRS:
        base = REPO_ROOT / scan_dir
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if path.is_file():
                yield path


@pytest.mark.parametrize("eol_family", EOL_GCE_IMAGE_FAMILIES)
def test_no_eol_gce_image_family_references(eol_family):
    family_path = f"global/images/family/{eol_family}"
    offenders = []
    for path in _iter_text_files():
        try:
            content = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        if family_path in content:
            offenders.append(str(path.relative_to(REPO_ROOT)))

    assert not offenders, f"found references to EOL GCE image family {eol_family!r} ({family_path!r}) in: {offenders}"
