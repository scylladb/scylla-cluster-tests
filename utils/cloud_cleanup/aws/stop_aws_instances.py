#!/usr/bin/env python

import argparse
import datetime
import os
import time
import logging
import sys
import boto3
import pytz


DRY_RUN = False
VERBOSE = False
TRACE = False
WAIT = 60


def is_running(instance):
    return instance.state['Name'] == 'running'


def is_stopped(instance):
    return instance.state['Name'] == 'stopped'


def is_terminated(instance):
    return instance.state['Name'] == 'terminated'


def debug(msg):
    if VERBOSE:
        print(msg)


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def since_now(since: datetime.datetime):
    return datetime.datetime.now(tz=pytz.utc) - since


def since_now_hours(since: datetime.datetime):
    return abs(since_now(since)).total_seconds() / 3600.0


def keep_alive_instance_duration(instance, duration):
    return duration > 0 and since_now_hours(instance.launch_time) < duration


def keep_alive_tag_val(instance):
    if instance.tags is not None:
        for tag in instance.tags:
            if tag['Key'].lower() == 'keep':
                return tag['Value']
    return ""


def keep_alive_action_tag_val(instance):
    if instance.tags is not None:
        for tag in instance.tags:
            if tag['Key'].lower() == 'keep_action':
                return tag['Value']
    return ""


def keep_alive_tag_int(instance):
    keep = keep_alive_tag_val(instance)
    if keep.isdigit():
        return int(keep)
    return 0


def should_terminate(instance):
    action = keep_alive_action_tag_val(instance)
    return TERMINATE if action == '' else action == 'terminate'

# check tags for keep:alive


def keep_alive_instance(instance):
    return keep_alive_tag_val(instance).lower() == 'alive'


# check tags for keep:num hours since instance creation
def keep_alive_instance_launch_time(instance):
    keep = keep_alive_tag_int(instance)
    return keep_alive_instance_duration(instance, keep)


def stop_instance(instance):
    try:
        if not DRY_RUN:
            instance.create_tags(Tags=[
                {
                    'Key': 'keep_alive_action',
                    'Value': 'stop'
                }
            ])
            instance.stop()
    except Exception as exc:  # pylint: disable=broad-except
        eprint("stop instance %s error: %s" % (instance.id, str(exc)))


def remove_protection(instance):
    try:

        if not DRY_RUN:
            instance.modify_attribute(
                DisableApiTermination={
                    'Value': False
                })
            print_instance(instance, "Disabling API Termination protection")
    except Exception as exc:  # pylint: disable=broad-except
        eprint("DisableApiTermination protection %s error: %s" % (instance.id, str(exc)))


def terminate_instance(instance):
    try:
        if not DRY_RUN:
            instance.create_tags(Tags=[
                {
                    'Key': 'keep_alive_action',
                    'Value': 'terminate'
                }
            ])
            instance.terminate()
    except Exception as exc:  # pylint: disable=broad-except
        eprint("terminate instance %s error: %s" % (instance.id, str(exc)))


def print_instance(instance, msg):
    print("instance %s type %s launched at %s keys %s %s" % (
        instance.id, instance.instance_type,
        instance.launch_time, instance.key_name,
        msg))


def regions_names():
    session = boto3.Session()
    default_region = session.region_name
    if not default_region:
        default_region = "eu-central-1"
    client = session.client('ec2', region_name=default_region)
    return [region['RegionName'] for region in client.describe_regions()['Regions']]


def scan_region_instances(region_name, duration):
    ec2 = boto3.resource('ec2', region_name=region_name)
    instances = ec2.instances.filter(Filters=[{
        'Name': 'instance-state-name',
        'Values': ['running']
    }])

    running, keep_alive, expiring, expired = 0, 0, 0, []

    for instance in instances:
        debug("checking instance %s, type %s, tags %s, placement %s, launch_time %s" % (
            instance.id, instance.instance_type, instance.tags, instance.placement, instance.launch_time))

        kalive = keep_alive_instance(instance)  # keep:alive
        ka_lt = keep_alive_instance_launch_time(instance)  # keep:X_HOURS
        ka_min = keep_alive_instance_duration(instance, duration)

        if kalive or ka_lt or ka_min:
            if kalive or ka_lt:
                keep_alive = keep_alive + 1
                if VERBOSE:
                    print_instance(instance, "keep alive")
            if ka_lt:
                expiring = expiring + 1
            running = running + 1
            debug("will not be stopped")
        else:
            expired.append(instance)

    return running, keep_alive, expiring, expired


def clean_region(region_name, duration):
    running, keep_alive, expiring, expired = scan_region_instances(region_name, duration)
    terminated = 0
    # stop instances
    for instance in expired:
        if is_running(instance):
            if should_terminate(instance):
                print_instance(instance, "terminating...")
                terminated = terminated + 1
                remove_protection(instance)
                terminate_instance(instance)
            else:
                print_instance(instance, "stopping...")
                remove_protection(instance)
                stop_instance(instance)

    # blind wait stop timeout
    if len(expired) > 0 and not DRY_RUN:
        print("region %s: waiting %d instances to stop" % (region_name, len(expired)))
        time.sleep(WAIT)

    # check instances are stopped. if they are not - terminate them
    for instance in expired:
        instance.reload()
        if not is_stopped(instance) and not is_terminated(instance):
            print_instance(instance, "terminating...")
            terminate_instance(instance)

    print("region %s: stopped %d, terminated %d, kept alive %d and expiring %d, running %d instances" % (
        region_name, len(expired) - terminated, terminated, keep_alive, expiring, running))


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser('ec2_stop')
    arg_parser.add_argument("duration", type=int,
                            help="duration to keep non-tagged instances running in hours",
                            default=os.environ.get('DURATION', None))
    arg_parser.add_argument("--verbose", action="store_true",
                            help="print processing instances details",
                            default=os.environ.get('VERBOSE', False))
    arg_parser.add_argument("--trace", action="store_true",
                            help="trace every AWS call",
                            default=os.environ.get('TRACE', False))
    arg_parser.add_argument("--wait", type=int,
                            help="blind wait for instances to stop timeout",
                            default=os.environ.get('WAIT', 60))
    arg_parser.add_argument("--dry-run", action="store_true",
                            help="do not stop or terminate anything",
                            default=os.environ.get('DRY_RUN', False))
    arg_parser.add_argument("--default-action",
                            help="The default action when stopping an image (stop/terminate)",
                            default=os.environ.get('DEFAULT_ACTION', "stop"))

    arguments = arg_parser.parse_args()

    VERBOSE = bool(arguments.verbose)
    WAIT = int(arguments.wait)
    DRY_RUN = bool(arguments.dry_run)
    TRACE = bool(arguments.trace)
    TERMINATE = arguments.default_action == 'terminate'

    if DRY_RUN:
        print("dry mode on")

    if TRACE:
        boto3.set_stream_logger(name='botocore')
        logging.getLogger('botocore').setLevel(logging.DEBUG)

    for region in regions_names():
        clean_region(region, arguments.duration)
