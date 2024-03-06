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

def keep_alive_address(eip_dict):
    # checking tags
    if "Tags" not in eip_dict:
        return False
    for tag in eip_dict["Tags"]:
        if tag['Key'] == 'keep' and tag['Value'] == 'alive':
            return True
    return False

def release_address(eip_dict, client):
    try:
        print_adress(eip_dict, "deleting")
        if not DRY_RUN:
            if "AllocationId" in eip_dict:
                client.release_address(AllocationId=eip_dict['AllocationId'])
            elif "PublicIp" in eip_dict:
                client.release_address(PublicIp=eip_dict['PublicIp'])
    except Exception as e:
        print(e)
        pass

def print_adress(eip_dict, msg):
    if "AllocationId" in eip_dict:
        print("Address ID %s %s" % (eip_dict["AllocationId"], msg))
    elif "PublicIp" in eip_dict:
        print("Address IP %s %s" % (eip_dict["PublicIp"], msg))
    
    
def check_region(name):
    client =  boto3.client('ec2',region_name=name)
    addresses_dict = client.describe_addresses()
    deleted_addresses = 0
    kept_addresses = 0    
    for eip_dict in addresses_dict['Addresses']:
        debug(eip_dict)
        if "NetworkInterfaceId" not in eip_dict and not keep_alive_address(eip_dict):
            deleted_addresses = deleted_addresses + 1
            release_address(eip_dict, client)
        else:
            kept_addresses = kept_addresses + 1
    print("region %s deleted %d ip addresses kept %s" % (name, deleted_addresses, kept_addresses))

def regions_names():
    session = boto3.Session()
    default_region = session.region_name
    if not default_region:
        default_region = "eu-central-1"
    client = session.client('ec2', region_name=default_region)
    return [region['RegionName'] for region in client.describe_regions()['Regions']]

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description='A helper tool to clear unused ip address')
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
