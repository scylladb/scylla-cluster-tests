import argparse
import os
from datetime import datetime, timedelta

from pytz import utc

from sdcm.utils.context_managers import environment
from sdcm.utils.gce_utils import get_gce_compute_instances_client, SUPPORTED_PROJECTS
from sdcm.utils.log import setup_stdout_logger

DEFAULT_KEEP_HOURS = 14
LOGGER = setup_stdout_logger()


def should_keep(creation_time, keep_hours):
    if keep_hours <= 0:
        return True
    try:
        keep_date = creation_time + timedelta(hours=keep_hours)
        now = datetime.utcnow()

        return now < keep_date
    except (TypeError, ValueError) as exc:
        LOGGER.info("error while defining if should keep: %s. Keeping.", exc)
        return True


def get_keep_hours(instance_metadata, default=DEFAULT_KEEP_HOURS):
    keep = instance_metadata.get('keep', "").lower() if instance_metadata else None
    if keep == "alive":
        return -1
    try:
        return int(keep)
    except (ValueError, TypeError):
        return default


def clean_gce_instances(instances_client, project_id, dry_run):
    zones = instances_client.aggregated_list(project=project_id)
    for zone in zones:
        for instance in zone[1].instances:
            vm_creation_time = datetime.fromisoformat(instance.creation_timestamp).astimezone(utc).replace(tzinfo=None)
            instance_metadata = {item.key: item.value for item in instance.metadata.items}
            instance_metadata.update(instance.labels)
            if "keep-alive" in instance.tags.items:
                instance_metadata.update({"keep": "alive"})
            if should_keep(vm_creation_time, get_keep_hours(instance_metadata)):
                LOGGER.info("keeping instance %s, keep: %s, creation time: %s ", instance.name,
                            instance_metadata.get('keep', 'not set'), vm_creation_time)
                continue
            if instance_metadata.get('keep_action', 'terminate') == 'terminate':  # terminate by default if not set
                if not dry_run:
                    LOGGER.info("terminating instance %s, creation time: %s", instance.name, vm_creation_time)
                    try:
                        res = instances_client.delete(instance=instance.name,
                                                      project=project_id,
                                                      zone=instance.zone.split('/')[-1])
                        res.done()
                        LOGGER.info("%s terminated", instance.name)
                    except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                        LOGGER.error("error while terminating instance %s: %s", instance.name, exc)
                else:
                    LOGGER.info("dry run: would terminate instance %s, creation time: %s",
                                instance.name, vm_creation_time)

            elif instance.status == 'RUNNING':  # stop the rest, only running instances
                if not dry_run:
                    LOGGER.info("stopping instance %s, creation time: %s", instance.name, vm_creation_time)
                    try:
                        res = instances_client.stop(instance=instance.name,
                                                    project=project_id,
                                                    zone=instance.zone.split('/')[-1])
                        res.done()
                        LOGGER.info("%s stopped", instance.name)
                    except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                        LOGGER.error("error while stopping instance %s: %s", instance.name, exc)
                else:
                    LOGGER.info("dry run: would stop instance %s, creation time: %s", instance.name, vm_creation_time)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser('gce_cleanup')
    arg_parser.add_argument("--duration", type=int,
                            help="duration to keep non-tagged instances running in hours",
                            default=os.environ.get('DURATION', str(DEFAULT_KEEP_HOURS)))
    arg_parser.add_argument("--dry-run", action=argparse.BooleanOptionalAction,
                            help="do not stop or terminate anything",
                            default=os.environ.get('DRY_RUN'))

    args = arg_parser.parse_args()

    is_dry_run = bool(args.dry_run)
    DEFAULT_KEEP_HOURS = int(args.duration)

    if is_dry_run:
        LOGGER.error("'Dry run' mode on")

    for project in SUPPORTED_PROJECTS:
        with environment(SCT_GCE_PROJECT=project):
            client, info = get_gce_compute_instances_client()
            clean_gce_instances(instances_client=client, project_id=info['project_id'], dry_run=is_dry_run)
