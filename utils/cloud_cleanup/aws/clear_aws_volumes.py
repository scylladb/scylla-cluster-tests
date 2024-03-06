#!/usr/bin/env python

import boto3
import datetime
import pytz
import time
import argparse

VERBOSE = False
DRY_RUN = False

def debug(str):
    if VERBOSE:
        print(str)
    return

def keep_alive_volume(volume):
    # checking tags
    if volume.tags == None:
       return False
    for tag in volume.tags:
       if tag['Key'] == 'keep' and tag['Value'] == 'alive':
          return True
    return False

def delete_volume(volume):
    try:
       print_volume(volume, "deleting")
       if not DRY_RUN:
           volume.delete()
    except Exception as e:
       pass

def print_volume(volume, msg):
    print ("volume %s %s" % (volume.id,msg))
    
def check_region(name):
    ec2 =  boto3.resource('ec2',region_name=name)

    volumes = ec2.volumes.all() 

    count_kept_volume = 0
    count_long_running = 0
    count_deleted_volume = 0
    for volume in volumes:
        debug("checking volume %s %s %s %s" % (volume.id, volume.volume_id, volume.state, volume.tags))
        ka = keep_alive_volume(volume)
        if ka or volume.state == "in-use":
           count_kept_volume = count_kept_volume + 1
           if VERBOSE:
               print_volume(volume,"kept")
        else:
           count_deleted_volume = count_deleted_volume + 1
           delete_volume(volume)
           if VERBOSE:
               print_volume(volume,"deleted")

    print("region %s deleted %d kept %d" % (name, count_deleted_volume, count_kept_volume))

def regions_names():
    session = boto3.Session()
    default_region = session.region_name
    if not default_region:
        default_region = "eu-central-1"
    client = session.client('ec2', region_name=default_region)
    return [region['RegionName'] for region in client.describe_regions()['Regions']]

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description='A helper tool to clear unused volume')
    arg_parser.add_argument("--verbose", action="store_true",
                            help="print processing instances details",
                            default=False)
    arg_parser.add_argument("--dry-run", action="store_true",
                            help="do not stop or terminate anything",
                            default=False)
    
    args = arg_parser.parse_args()
    VERBOSE = args.verbose
    DRY_RUN = args.dry_run
    for region in regions_names():
       check_region(region)
