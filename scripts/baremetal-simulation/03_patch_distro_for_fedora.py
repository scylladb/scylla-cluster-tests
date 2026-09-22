#!/usr/bin/env python3
"""Step 3 -- teach SCT about recent Fedora releases (local, uncommitted patch).

    uv run python scripts/baremetal-simulation/03_patch_distro_for_fedora.py --check
    uv run python scripts/baremetal-simulation/03_patch_distro_for_fedora.py --apply
    uv run python scripts/baremetal-simulation/03_patch_distro_for_fedora.py --revert

sdcm/utils/distro.py declares:

    ("FEDORA", "fedora", ["34", "35", "36"], DistroBase.RHEL)

On Fedora 44 the enum therefore resolves to Distro.UNKNOWN, is_rhel_like is False,
and sdcm/cluster.py:2845 install_scylla() falls through to the Debian/apt branch --
the run dies on an apt error minutes in, on an RPM host.

This is the one real code finding of the simulation.  The patch belongs in its own
PR (with a unit test); this script only makes the experiment runnable meanwhile,
and --revert puts the file back the way it was.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import REPO_ROOT, log  # noqa: E402

DISTRO_PY = REPO_ROOT / "sdcm" / "utils" / "distro.py"
ORIGINAL_VERSIONS = ["34", "35", "36"]
PATCHED_VERSIONS = ["34", "35", "36", "41", "42", "43", "44", "45"]
LINE_RE = re.compile(r'^(\s*\("FEDORA", "fedora", )\[[^\]]*\](, DistroBase\.RHEL\),\s*)$', re.MULTILINE)


def current_versions(text: str) -> list[str]:
    line = next((ln for ln in text.splitlines() if '"FEDORA"' in ln), "")
    return re.findall(r'"(\d+)"', line)


def rewrite(versions: list[str]) -> bool:
    text = DISTRO_PY.read_text(encoding="utf-8")
    if current_versions(text) == versions:
        log(f"{DISTRO_PY.relative_to(REPO_ROOT)} already lists Fedora {versions}")
        return False
    replacement = ", ".join(f'"{version}"' for version in versions)
    new_text, count = LINE_RE.subn(rf"\g<1>[{replacement}]\g<2>", text)
    if count != 1:
        raise SystemExit(f"could not locate the FEDORA entry in {DISTRO_PY} -- patch it by hand")
    DISTRO_PY.write_text(new_text, encoding="utf-8")
    log(f"{DISTRO_PY.relative_to(REPO_ROOT)}: Fedora versions -> {versions}")
    return True


def verify() -> None:
    """Import the patched module and check a Fedora 44 os-release resolves."""
    sys.path.insert(0, str(REPO_ROOT))
    from sdcm.utils.distro import Distro  # noqa: PLC0415

    distro = Distro.from_os_release('ID=fedora\nVERSION_ID="44"\n')
    log(f"Distro.from_os_release(fedora 44) -> {distro} (is_rhel_like={distro.is_rhel_like})")
    if not distro.is_rhel_like:
        raise SystemExit("Fedora 44 still is not rhel-like -- the RPM install path will not be taken")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--check", action="store_true", help="report the versions currently declared")
    group.add_argument("--apply", action="store_true", help="add Fedora 41-45")
    group.add_argument("--revert", action="store_true", help="restore the upstream list")
    args = parser.parse_args()

    versions = current_versions(DISTRO_PY.read_text(encoding="utf-8"))
    if args.check:
        log(f"{DISTRO_PY.relative_to(REPO_ROOT)} declares Fedora {versions}")
        patched = any(int(version) >= 41 for version in versions)
        log("patched" if patched else "NOT patched -- run with --apply before the test")
        return 0 if patched else 1

    if args.apply:
        rewrite(PATCHED_VERSIONS)
        verify()
        log("next: 04_render_test_case.py")
        return 0

    rewrite(ORIGINAL_VERSIONS)
    log("reverted; `git diff sdcm/utils/distro.py` should now be empty")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
