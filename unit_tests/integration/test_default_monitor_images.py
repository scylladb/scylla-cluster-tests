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

"""Integration tests asserting the default monitoring images actually exist.

The pipeline linter (``sct.py lint-pipelines``) stubs out cloud image resolution so it can
run without credentials, which means a monitoring image bump can merge while pointing at an
image that was never published. That happened with
``scylladb-monitor-4-16-0-amd64-2026-08-31t09-45-49z`` (SCT-910): only the arm64 half of that
build reached AWS, so every AWS and GCE run failed to provision the monitor node.

These tests close that gap by resolving the image names from ``defaults/aws_config.yaml`` and
``defaults/gce_config.yaml`` against the real cloud APIs. An AMI must be present in *every*
region SCT provisions in — a partial copy (image in us-east-1 only) is as broken as no image
at all, and is the more likely failure mode since the images are copied region by region.

External services: AWS EC2 (DescribeImages, all supported regions), GCE Compute (Images).

Run with::

    ./docker/env/hydra.sh integration-tests -t integration/test_default_monitor_images.py
"""

import anyconfig
import pytest
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
from google.api_core.exceptions import GoogleAPIError, NotFound

from sdcm import sct_abs_path
from sdcm.sct_config import AWS_SUPPORTED_REGIONS
from sdcm.utils.common import convert_name_to_ami_if_needed
from sdcm.utils.gce_utils import GCE_IMAGE_URL_REGEX, get_gce_compute_images_client


pytestmark = pytest.mark.integration


def _default_value(config_file: str, key: str) -> str:
    """Read a single value out of one of the ``defaults/*_config.yaml`` files.

    Args:
        config_file: File name under ``defaults/``, e.g. ``aws_config.yaml``.
        key: Config option to read, e.g. ``ami_id_monitor``.

    Returns:
        The configured value, stripped of surrounding whitespace.
    """
    defaults = anyconfig.load(sct_abs_path(f"defaults/{config_file}"))
    value = (defaults.get(key) or "").strip()
    assert value, f"'{key}' is not set in defaults/{config_file}"
    return value


def test_monitor_ami_exists_in_all_supported_regions():
    """`ami_id_monitor` must resolve to an AMI in every region SCT provisions in.

    Resolution goes through the same `convert_name_to_ami_if_needed` call `SCTConfiguration`
    makes at provisioning time, one region at a time so that a partial region copy is
    reported per-region instead of as a single opaque failure.

    External services: AWS EC2 DescribeImages.
    """
    ami_id_monitor = _default_value("aws_config.yaml", "ami_id_monitor")

    resolved = {}
    missing = []
    for region in AWS_SUPPORTED_REGIONS:
        try:
            resolved[region] = convert_name_to_ami_if_needed(ami_id_monitor, (region,))
        except ValueError:
            missing.append(region)
        except (ClientError, NoCredentialsError, BotoCoreError) as exc:
            pytest.skip(f"AWS API error while looking up '{ami_id_monitor}' in {region}: {exc}")

    assert not missing, (
        f"monitor image '{ami_id_monitor}' (defaults/aws_config.yaml) is missing in "
        f"{len(missing)} of {len(AWS_SUPPORTED_REGIONS)} supported regions: {', '.join(missing)}. "
        f"Found in: {', '.join(resolved) or 'none'}. "
        f"Copy the AMI to the missing regions (see utils/copy_ami_to_all_regions.sh) "
        f"before bumping ami_id_monitor."
    )


def test_monitor_gce_image_exists():
    """`gce_image_monitor` must point at an image that exists and is READY.

    External services: GCE Compute Images.
    """
    gce_image_monitor = _default_value("gce_config.yaml", "gce_image_monitor")

    match = GCE_IMAGE_URL_REGEX.search(gce_image_monitor)
    assert match, (
        f"monitor image '{gce_image_monitor}' (defaults/gce_config.yaml) is not a GCE image URL "
        f"of the form https://www.googleapis.com/compute/v1/projects/<project>/global/images/<image>"
    )
    image_params = match.groupdict()

    images_client, _ = get_gce_compute_images_client()
    try:
        image = images_client.get(**image_params)
    except NotFound:
        pytest.fail(
            f"monitor image '{image_params['image']}' does not exist in GCE project "
            f"'{image_params['project']}' (defaults/gce_config.yaml). "
            f"Publish the image before bumping gce_image_monitor."
        )
    except GoogleAPIError as exc:
        pytest.skip(f"GCE API error while looking up '{gce_image_monitor}': {exc}")

    assert image.status == "READY", (
        f"monitor image '{image_params['image']}' in GCE project '{image_params['project']}' "
        f"is in status '{image.status}', expected 'READY'"
    )
