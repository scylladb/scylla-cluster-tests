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
# Copyright (c) 2017 ScyllaDB

# pylint: disable=too-many-lines
import itertools
import os
import logging
import random
import socket
import time
import datetime
import errno
import threading
import select
import shutil

from functools import wraps
from enum import Enum
from collections import defaultdict
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from urlparse import urlparse
import hashlib

import boto3
import libcloud.storage.providers
import libcloud.storage.types
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider

LOGGER = logging.getLogger('utils')


def _remote_get_hash(remoter, file_path):
    try:
        result = remoter.run('md5sum {}'.format(file_path), verbose=True)
        return result.stdout.strip().split()[0]
    except Exception as details:  # pylint: disable=broad-except
        LOGGER.error(str(details))
        return None


def _remote_get_file(remoter, src, dst, user_agent=None):
    cmd = 'curl -L {} -o {}'.format(src, dst)
    if user_agent:
        cmd += ' --user-agent %s' % user_agent
    return remoter.run(cmd, ignore_status=True)


def remote_get_file(remoter, src, dst, hash_expected=None, retries=1, user_agent=None):  # pylint: disable=too-many-arguments
    _remote_get_file(remoter, src, dst, user_agent)
    if not hash_expected:
        return
    while retries > 0 and _remote_get_hash(remoter, dst) != hash_expected:
        _remote_get_file(remoter, src, dst, user_agent)
        retries -= 1
    assert _remote_get_hash(remoter, dst) == hash_expected


class retrying(object):  # pylint: disable=invalid-name,too-few-public-methods
    """
        Used as a decorator to retry function run that can possibly fail with allowed exceptions list
    """

    def __init__(self, n=3, sleep_time=1, allowed_exceptions=(Exception,), message=""):
        assert n > 0, "Number of retries parameter should be greater then 0 (current: %s)" % n
        self.n = n  # number of times to retry  # pylint: disable=invalid-name
        self.sleep_time = sleep_time  # number seconds to sleep between retries
        self.allowed_exceptions = allowed_exceptions  # if Exception is not allowed will raise
        self.message = message  # string that will be printed between retries

    def __call__(self, func):
        @wraps(func)
        def inner(*args, **kwargs):
            if self.n == 1:
                # there is no need to retry
                return func(*args, **kwargs)
            for i in xrange(self.n):
                try:
                    if self.message:
                        LOGGER.info("%s [try #%s]", self.message, i)
                    return func(*args, **kwargs)
                except self.allowed_exceptions as ex:
                    LOGGER.debug("'%s': failed with '%r', retrying [#%s]", func.func_name, ex, i)
                    time.sleep(self.sleep_time)
                    if i == self.n - 1:
                        LOGGER.error("'%s': Number of retries exceeded!", func.func_name)
                        raise
        return inner


def log_run_info(arg):
    """
        Decorator that prints BEGIN before the function runs and END when function finished running.
        Uses function name as a name of action or string that can be given to the decorator.
        If the function is a method of a class object, the class name will be printed out.

        Usage examples:
            @log_run_info
            def foo(x, y=1):
                pass
            In: foo(1)
            Out:
                BEGIN: foo
                END: foo (ran 0.000164)s

            @log_run_info("Execute nemesis")
            def disrupt():
                pass
            In: disrupt()
            Out:
                BEGIN: Execute nemesis
                END: Execute nemesis (ran 0.000271)s
    """
    def _inner(func, msg=None):
        @wraps(func)
        def inner(*args, **kwargs):
            class_name = ""
            if args and func.__name__ in dir(args[0]):
                class_name = " <%s>" % args[0].__class__.__name__
            action = "%s%s" % (msg, class_name)
            start_time = datetime.datetime.now()
            LOGGER.debug("BEGIN: %s", action)
            res = func(*args, **kwargs)
            end_time = datetime.datetime.now()
            LOGGER.debug("END: %s (ran %ss)", action, (end_time - start_time).total_seconds())
            return res
        return inner

    if callable(arg):  # when decorator is used without a string message
        return _inner(arg, arg.__name__)
    else:
        return lambda f: _inner(f, arg)


class Distro(Enum):
    UNKNOWN = 0
    CENTOS7 = 1
    RHEL7 = 2
    UBUNTU14 = 3
    UBUNTU16 = 4
    UBUNTU18 = 5
    DEBIAN8 = 6
    DEBIAN9 = 7


def get_data_dir_path(*args):
    import sdcm
    sdcm_path = os.path.realpath(sdcm.__path__[0])
    data_dir = os.path.join(sdcm_path, "../data_dir", *args)
    return os.path.abspath(data_dir)


def get_job_name():
    return os.environ.get('JOB_NAME', 'local_run')


def verify_scylla_repo_file(content, is_rhel_like=True):
    LOGGER.info('Verifying Scylla repo file')
    if is_rhel_like:
        body_prefix = ['#', '[scylla', 'name=', 'baseurl=', 'enabled=', 'gpgcheck=', 'type=',
                       'skip_if_unavailable=', 'gpgkey=', 'repo_gpgcheck=', 'enabled_metadata=']
    else:
        body_prefix = ['#', 'deb']
    for line in content.split('\n'):
        valid_prefix = False
        for prefix in body_prefix:
            if line.startswith(prefix) or not line.strip():
                valid_prefix = True
                break
        LOGGER.debug(line)
        assert valid_prefix, 'Repository content has invalid line: {}'.format(line)


def remove_comments(data):
    """Remove comments line from data

    Remove any string which is start from # in data

    Arguments:
        data {str} -- data expected the command output, file contents
    """
    return '\n'.join([i.strip() for i in data.split('\n') if not i.startswith('#')])


class S3Storage(object):

    bucket_name = 'cloudius-jenkins-test'
    enable_multipart_threshold_size = 1024 * 1024 * 1024  # 1GB
    multipart_chunksize = 50 * 1024 * 1024  # 50 MB
    num_download_attempts = 5

    def __init__(self, bucket=None):
        if bucket:
            self.bucket_name = bucket
        self._bucket = boto3.resource("s3").Bucket(name=self.bucket_name)
        self.transfer_config = boto3.s3.transfer.TransferConfig(multipart_threshold=self.enable_multipart_threshold_size,
                                                                multipart_chunksize=self.multipart_chunksize,
                                                                num_download_attempts=self.num_download_attempts)

    def get_s3_fileojb(self, key):
        objects = []
        for obj in self._bucket.objects.filter(Prefix=key):
            objects.append(obj)
        return objects

    def search_by_path(self, path=''):
        files = []
        for obj in self._bucket.objects.filter(Prefix=path):
            files.append(obj.key)
        return files

    def generate_url(self, file_path, dest_dir=''):
        bucket_name = self.bucket_name
        file_name = os.path.basename(os.path.normpath(file_path))
        return "https://{bucket_name}.s3.amazonaws.com/{dest_dir}/{file_name}".format(dest_dir=dest_dir,
                                                                                      file_name=file_name,
                                                                                      bucket_name=bucket_name)

    def upload_file(self, file_path, dest_dir=''):
        s3_url = self.generate_url(file_path, dest_dir)
        try:
            LOGGER.info("Uploading '{file_path}' to {s3_url}".format(file_path=file_path, s3_url=s3_url))
            print "Uploading '{file_path}' to {s3_url}".format(file_path=file_path, s3_url=s3_url)
            self._bucket.upload_file(Filename=file_path,
                                     Key="{}/{}".format(dest_dir, os.path.basename(file_path)),
                                     Config=self.transfer_config)
            LOGGER.info("Uploaded to {0}".format(s3_url))
            return s3_url
        except Exception as details:  # pylint: disable=broad-except
            LOGGER.debug("Unable to upload to S3: %s", details)
            return ""

    def download_file(self, link, dst_dir):
        key_name = link.replace("https://{0.bucket_name}.s3.amazonaws.com/".format(self), "")
        file_name = os.path.basename(key_name)
        try:
            LOGGER.info("Downloading {0} from {1}".format(key_name, self.bucket_name))
            self._bucket.download_file(Key=key_name,
                                       Filename=os.path.join(dst_dir, file_name),
                                       Config=self.transfer_config)
            LOGGER.info("Downloaded finished")
            return os.path.join(os.path.abspath(dst_dir), file_name)

        except Exception as details:  # pylint: disable=broad-except
            LOGGER.warning("File {} is not downloaded by reason: {}".format(key_name, details))
            return ""


def get_latest_gemini_version():
    bucket_name = 'downloads.scylladb.com'

    results = S3Storage(bucket_name).search_by_path(path='gemini')
    versions = set()
    for result_file in results:
        versions.add(result_file.split('/')[1])

    return str(sorted(versions)[-1])


def list_logs_by_test_id(test_id):
    log_types = ['db-cluster', 'monitor-set', 'loader-set', 'sct-runner',
                 'prometheus', 'grafana',
                 'job', 'monitoring_data_stack', 'events']
    results = []

    if not test_id:
        return results

    def convert_to_date(date_str):
        try:
            t = datetime.datetime.strptime(date_str, "%Y%m%d_%H%M%S")  # pylint: disable=invalid-name
        except ValueError:
            try:
                t = datetime.datetime.strptime(date_str, "%Y_%m_%d_%H_%M_%S")  # pylint: disable=invalid-name
            except ValueError:
                t = datetime.datetime(1999, 1, 1, 1, 1, 1)  # pylint: disable=invalid-name
        return t   # pylint: disable=invalid-name

    log_files = S3Storage().search_by_path(path=test_id)
    for log_file in log_files:
        for log_type in log_types:
            if log_type in log_file:
                results.append({"file_path": log_file,
                                "type": log_type,
                                "link": "https://{}.s3.amazonaws.com/{}".format(S3Storage.bucket_name, log_file),
                                "date": convert_to_date(log_file.split('/')[1])
                                })
                break
    results = sorted(results, key=lambda x: x["date"])

    return results


def all_aws_regions():
    client = boto3.client('ec2')
    return [region['RegionName'] for region in client.describe_regions()['Regions']]


AWS_REGIONS = all_aws_regions()


class ParallelObject(object):  # pylint: disable=too-few-public-methods
    """
        Run function in with supplied args in parallel using thread.
    """

    def __init__(self, objects, timeout=6, num_workers=None, disable_logging=False):
        self.objects = objects
        self.timeout = timeout
        self.num_workers = num_workers
        self.disable_logging = disable_logging

    def run(self, func):

        def func_wrap(fun):
            def inner(*args, **kwargs):
                thread_name = threading.current_thread().name
                fun_args = args
                fun_kwargs = kwargs
                fun_name = fun.__name__
                LOGGER.debug("[{thread_name}] {fun_name}({fun_args}, {fun_kwargs})".format(thread_name=thread_name,
                                                                                           fun_name=fun_name,
                                                                                           fun_args=fun_args,
                                                                                           fun_kwargs=fun_kwargs))
                return_val = fun(*args, **kwargs)
                LOGGER.debug("[{thread_name}] Done.".format(thread_name=thread_name))
                return return_val
            return inner

        with ThreadPoolExecutor(max_workers=self.num_workers) as pool:
            LOGGER.debug("Executing in parallel: '{}' on {}".format(func.__name__, self.objects))
            if not self.disable_logging:
                func = func_wrap(func)
            return list(pool.map(func, self.objects, timeout=self.timeout))


def clean_cloud_instances(tags_dict):
    """
    Remove all instances with specific tags from both AWS/GCE

    :param tags_dict: a dict of the tag to select the instances,e.x. {"TestId": "9bc6879f-b1ef-47e1-99ab-020810aedbcc"}
    :return: None
    """
    clean_instances_aws(tags_dict)
    clean_elastic_ips_aws(tags_dict)
    clean_instances_gce(tags_dict)


def aws_tags_to_dict(tags_list):
    tags_dict = {}
    if tags_list:
        for item in tags_list:
            tags_dict[item["Key"]] = item["Value"]
    return tags_dict


def list_instances_aws(tags_dict=None, region_name=None, running=False, group_as_region=False, verbose=False):
    """
    list all instances with specific tags AWS

    :param tags_dict: a dict of the tag to select the instances, e.x. {"TestId": "9bc6879f-b1ef-47e1-99ab-020810aedbcc"}
    :param region_name: name of the region to list
    :param running: get all running instances
    :param group_as_region: if True the results would be grouped into regions
    :param verbose: if True will log progress information

    :return: instances dict where region is a key
    """
    instances = {}
    aws_regions = [region_name] if region_name else AWS_REGIONS

    def get_instances(region):
        if verbose:
            LOGGER.info('Going to list aws region "%s"', region)
        time.sleep(random.random())
        client = boto3.client('ec2', region_name=region)
        custom_filter = []
        if tags_dict:
            custom_filter = [{'Name': 'tag:{}'.format(key), 'Values': [value]} for key, value in tags_dict.items()]
        response = client.describe_instances(Filters=custom_filter)
        instances[region] = [instance for reservation in response['Reservations'] for instance in reservation[
            'Instances']]

        if verbose:
            LOGGER.info("%s: done [%s/%s]", region, len(instances.keys()), len(aws_regions))

    ParallelObject(aws_regions, timeout=100).run(get_instances)

    for curr_region_name in instances:
        if running:
            instances[curr_region_name] = [i for i in instances[curr_region_name] if i['State']['Name'] == 'running']
        else:
            instances[curr_region_name] = [i for i in instances[curr_region_name]
                                           if not i['State']['Name'] == 'terminated']
    if not group_as_region:
        instances = list(itertools.chain(*instances.values()))  # flatten the list of lists
        total_items = len(instances)
    else:
        total_items = sum([len(value) for _, value in instances.items()])

    if verbose:
        LOGGER.info("Found total of %s instances.", len(total_items))

    return instances


def clean_instances_aws(tags_dict):
    """
    Remove all instances with specific tags AWS

    :param tags_dict: a dict of the tag to select the instances, e.x. {"TestId": "9bc6879f-b1ef-47e1-99ab-020810aedbcc"}
    :return: None
    """
    assert tags_dict, "tags_dict not provided (can't clean all instances)"
    aws_instances = list_instances_aws(tags_dict=tags_dict, group_as_region=True)

    for region, instance_list in aws_instances.items():
        client = boto3.client('ec2', region_name=region)
        for instance in instance_list:
            tags = aws_tags_to_dict(instance.get('Tags'))
            name = tags.get("Name", "N/A")
            instance_id = instance['InstanceId']
            LOGGER.info("Going to delete '{instance_id}' [name={name}] ".format(instance_id=instance_id, name=name))
            response = client.terminate_instances(InstanceIds=[instance_id])
            LOGGER.debug("Done. Result: %s\n", response['TerminatingInstances'])


def list_elastic_ips_aws(tags_dict=None, region_name=None, group_as_region=False, verbose=False):
    """
    list all elastic ips with specific tags AWS

    :param tags_dict: a dict of the tag to select the instances, e.x. {"TestId": "9bc6879f-b1ef-47e1-99ab-020810aedbcc"}
    :param region_name: name of the region to list
    :param group_as_region: if True the results would be grouped into regions
    :param verbose: if True will log progress information

    :return: instances dict where region is a key
    """
    elastic_ips = {}
    aws_regions = [region_name] if region_name else AWS_REGIONS

    def get_elastic_ips(region):
        if verbose:
            LOGGER.info('Going to list aws region "%s"', region)
        time.sleep(random.random())
        client = boto3.client('ec2', region_name=region)
        custom_filter = []
        if tags_dict:
            custom_filter = [{'Name': 'tag:{}'.format(key), 'Values': [value]} for key, value in tags_dict.items()]
        response = client.describe_addresses(Filters=custom_filter)
        elastic_ips[region] = [ip for ip in response['Addresses']]
        if verbose:
            LOGGER.info("%s: done [%s/%s]", region, len(elastic_ips.keys()), len(aws_regions))

    ParallelObject(aws_regions, timeout=100).run(get_elastic_ips)

    if not group_as_region:
        elastic_ips = list(itertools.chain(*elastic_ips.values()))  # flatten the list of lists
        total_items = elastic_ips
    else:
        total_items = sum([len(value) for _, value in elastic_ips.items()])
    if verbose:
        LOGGER.info("Found total of %s ips.", total_items)
    return elastic_ips


def clean_elastic_ips_aws(tags_dict):
    """
    Remove all elastic ips with specific tags AWS

    :param tags_dict: a dict of the tag to select the instances, e.x. {"TestId": "9bc6879f-b1ef-47e1-99ab-020810aedbcc"}
    :return: None
    """
    assert tags_dict, "tags_dict not provided (can't clean all instances)"
    aws_instances = list_elastic_ips_aws(tags_dict=tags_dict, group_as_region=True)

    for region, eip_list in aws_instances.items():
        client = boto3.client('ec2', region_name=region)
        for eip in eip_list:
            association_id = eip.get('AssociationId', None)
            if association_id:
                response = client.disassociate_address(AssociationId=association_id)
                LOGGER.debug("disassociate_address. Result: %s\n", response)

            allocation_id = eip['AllocationId']
            LOGGER.info("Going to release '{allocation_id}' [public_ip={public_ip}] ".format(
                allocation_id=allocation_id, public_ip=eip['PublicIp']))
            response = client.release_address(AllocationId=allocation_id)
            LOGGER.debug("Done. Result: %s\n", response)


def get_all_gce_regions():
    from sdcm.keystore import KeyStore
    gcp_credentials = KeyStore().get_gcp_credentials()
    gce_driver = get_driver(Provider.GCE)

    compute_engine = gce_driver(gcp_credentials["project_id"] + "@appspot.gserviceaccount.com",
                                gcp_credentials["private_key"],
                                project=gcp_credentials["project_id"])
    all_gce_regions = [region_obj.name for region_obj in compute_engine.region_list]
    return all_gce_regions


def gce_meta_to_dict(metadata):
    meta_dict = {}
    data = metadata.get("items")
    if data:
        for item in data:
            key = item["key"]
            if key:  # sometimes key is empty string
                meta_dict[key] = item["value"]
    return meta_dict


def filter_gce_by_tags(tags_dict, instances):
    filtered_instances = []
    for instance in instances:
        tags = gce_meta_to_dict(instance.extra['metadata'])
        found_keys = set(k for k in tags_dict if k in tags and tags_dict[k] == tags[k])
        if found_keys == set(tags_dict.keys()):
            filtered_instances.append(instance)
    return filtered_instances


def list_instances_gce(tags_dict=None, running=False, verbose=False):
    """
    list all instances with specific tags GCE

    :param tags_dict: a dict of the tag to select the instances, e.x. {"TestId": "9bc6879f-b1ef-47e1-99ab-020810aedbcc"}

    :return: None
    """

    # avoid cyclic dependency issues, since too many things import utils.py
    from sdcm.keystore import KeyStore

    gcp_credentials = KeyStore().get_gcp_credentials()
    gce_driver = get_driver(Provider.GCE)

    compute_engine = gce_driver(gcp_credentials["project_id"] + "@appspot.gserviceaccount.com",
                                gcp_credentials["private_key"],
                                project=gcp_credentials["project_id"])

    if verbose:
        LOGGER.info("Going to get all instances from GCE")
    all_gce_instances = compute_engine.list_nodes()
    # filter instances by tags since libcloud list_nodes() doesn't offer any filtering
    if tags_dict:
        instances = filter_gce_by_tags(tags_dict=tags_dict, instances=all_gce_instances)
    else:
        instances = all_gce_instances

    if running:
        # https://libcloud.readthedocs.io/en/latest/compute/api.html#libcloud.compute.types.NodeState
        instances = [i for i in instances if i.state == 'running']
    else:
        instances = [i for i in instances if not i.state == 'terminated']
    if verbose:
        LOGGER.info("Done. Found total of %s instances.", len(instances))
    return instances


def clean_instances_gce(tags_dict):
    """
    Remove all instances with specific tags GCE

    :param tags_dict: a dict of the tag to select the instances, e.x. {"TestId": "9bc6879f-b1ef-47e1-99ab-020810aedbcc"}
    :return: None
    """
    assert tags_dict, "tags_dict not provided (can't clean all instances)"
    all_gce_instances = list_instances_gce(tags_dict=tags_dict)

    for instance in all_gce_instances:
        LOGGER.info("Going to delete: {}".format(instance.name))
        # https://libcloud.readthedocs.io/en/latest/compute/api.html#libcloud.compute.base.Node.destroy
        res = instance.destroy()
        LOGGER.info("{} deleted. res={}".format(instance.name, res))


_SCYLLA_AMI_CACHE = defaultdict(dict)


def get_scylla_ami_versions(region):
    """
    get the list of all the formal scylla ami from specific region

    :param region: the aws region to look in
    :return: list of ami data
    :rtype: list
    """

    if _SCYLLA_AMI_CACHE[region]:
        return _SCYLLA_AMI_CACHE[region]

    ec2 = boto3.client('ec2', region_name=region)
    response = ec2.describe_images(
        Owners=['797456418907'],  # ScyllaDB
        Filters=[
            {'Name': 'name', 'Values': ['ScyllaDB *']},
        ],
    )

    _SCYLLA_AMI_CACHE[region] = sorted(response['Images'],
                                       key=lambda x: x['CreationDate'],
                                       reverse=True)

    return _SCYLLA_AMI_CACHE[region]


_S3_SCYLLA_REPOS_CACHE = defaultdict(dict)


def get_s3_scylla_repos_mapping(dist_type='centos', dist_version=None):
    """
    get the mapping from version prefixes to rpm .repo or deb .list files locations

    :param dist_type: which distro to look up centos/ubuntu/debian
    :param dist_version: famaily name of the distro version

    :return: a mapping of versions prefixes to repos
    :rtype: dict
    """
    if (dist_type, dist_version) in _S3_SCYLLA_REPOS_CACHE:
        return _S3_SCYLLA_REPOS_CACHE[(dist_type, dist_version)]

    s3_client = boto3.client('s3')
    bucket = 'downloads.scylladb.com'

    if dist_type == 'centos':
        response = s3_client.list_objects(Bucket=bucket, Prefix='rpm/centos/', Delimiter='/')

        for repo_file in response['Contents']:
            filename = os.path.basename(repo_file['Key'])
            # only if path look like 'rpm/centos/scylla-1.3.repo', we deem it formal one
            if filename.startswith('scylla-') and filename.endswith('.repo'):
                version_prefix = filename.replace('.repo', '').split('-')[-1]
                _S3_SCYLLA_REPOS_CACHE[(
                    dist_type, dist_version)][version_prefix] = "https://s3.amazonaws.com/{bucket}/{path}".format(bucket=bucket, path=repo_file['Key'])

    elif dist_type == 'ubuntu' or dist_type == 'debian':
        response = s3_client.list_objects(Bucket=bucket, Prefix='deb/{}/'.format(dist_type), Delimiter='/')
        for repo_file in response['Contents']:
            filename = os.path.basename(repo_file['Key'])

            # only if path look like 'deb/debian/scylla-3.0-jessie.list', we deem it formal one
            if filename.startswith('scylla-') and filename.endswith('-{}.list'.format(dist_version)):

                version_prefix = filename.replace('-{}.list'.format(dist_version), '').split('-')[-1]
                _S3_SCYLLA_REPOS_CACHE[(
                    dist_type, dist_version)][version_prefix] = "https://s3.amazonaws.com/{bucket}/{path}".format(bucket=bucket, path=repo_file['Key'])

    else:
        raise NotImplementedError("[{}] is not yet supported".format(dist_type))
    return _S3_SCYLLA_REPOS_CACHE[(dist_type, dist_version)]


def pid_exists(pid):
    """
    Return True if a given PID exists.

    :param pid: Process ID number.
    """
    try:
        os.kill(pid, 0)
    except OSError as detail:
        if detail.errno == errno.ESRCH:
            return False
    return True


def safe_kill(pid, signal):
    """
    Attempt to send a signal to a given process that may or may not exist.

    :param signal: Signal number.
    """
    try:
        os.kill(pid, signal)
        return True
    except Exception:  # pylint: disable=broad-except
        return False


class FileFollowerIterator(object):  # pylint: disable=too-few-public-methods
    def __init__(self, filename, thread_obj):
        self.filename = filename
        self.thread_obj = thread_obj

    def __iter__(self):
        with open(self.filename, 'r') as input_file:
            line = ''
            while not self.thread_obj.stopped():
                poller = select.poll()  # pylint: disable=no-member
                poller.register(input_file, select.POLLIN)  # pylint: disable=no-member
                if poller.poll(100):
                    line += input_file.readline()
                if not line or not line.endswith('\n'):
                    time.sleep(0.1)
                    continue

                yield line
                line = ''
            yield line


class FileFollowerThread(object):
    def __init__(self):
        self.executor = concurrent.futures.ThreadPoolExecutor(1)
        self._stop_event = threading.Event()
        self.future = None

    def __enter__(self):
        self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def run(self):
        raise NotImplementedError()

    def start(self):
        self.future = self.executor.submit(self.run)
        return self.future

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    def follow_file(self, filename):
        return FileFollowerIterator(filename, self)


class ScyllaCQLSession(object):
    def __init__(self, session, cluster):
        self.session = session
        self.cluster = cluster

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cluster.shutdown()


class MethodVersionNotFound(Exception):
    pass


class version(object):  # pylint: disable=invalid-name,too-few-public-methods
    VERSIONS = {}
    """
        Runs a method according to the version attribute of the class method
        Limitations: currently, can't work if the same method name in the same file used in different
                     classes
        Example:
                In [3]: class VersionedClass(object):
                   ...:     def __init__(self, current_version):
                   ...:         self.version = current_version
                   ...:
                   ...:     @version("1.2")
                   ...:     def setup(self):
                   ...:         return "1.2"
                   ...:
                   ...:     @version("2")
                   ...:     def setup(self):
                   ...:         return "2"

                In [4]: vc = VersionedClass("2")

                In [5]: vc.setup()
                Out[5]: '2'

                In [6]: vc = VersionedClass("1.2")

                In [7]: vc.setup()
                Out[7]: '1.2'
    """

    def __init__(self, ver):
        self.version = ver

    def __call__(self, func):
        self.VERSIONS[(self.version, func.func_name, func.func_code.co_filename)] = func

        @wraps(func)
        def inner(*args, **kwargs):
            cls_self = args[0]
            func_to_run = self.VERSIONS.get((cls_self.version, func.func_name, func.func_code.co_filename))
            if func_to_run:
                return func_to_run(*args, **kwargs)
            else:
                raise MethodVersionNotFound("Method '{}' with version '{}' not defined in '{}'!".format(
                    func.func_name,
                    cls_self.version,
                    cls_self.__class__.__name__))
        return inner


def get_free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    addr = sock.getsockname()
    port = addr[1]
    sock.close()
    return port


def get_my_ip():
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    return ip


def get_branched_ami(ami_version, region_name):
    """
    Get a list of AMIs, based on version match

    :param ami_version: branch version to look for, ex. 'branch-2019.1:latest', 'branch-3.1:all'
    :param region_name: the region to look AMIs in
    :return: list of ec2.images
    """
    branch, build_id = ami_version.split(':')
    ec2 = boto3.resource('ec2', region_name=region_name)

    LOGGER.info("Looking for AMI match [%s]", ami_version)
    if build_id == 'latest' or build_id == 'all':
        filters = [{'Name': 'tag:branch', 'Values': [branch]}]
    else:
        filters = [{'Name': 'tag:branch', 'Values': [branch]}, {'Name': 'tag:build-id', 'Values': [build_id]}]

    amis = list(ec2.images.filter(Filters=filters))

    amis = sorted(amis, key=lambda x: x.creation_date, reverse=True)

    assert amis, "AMI matching [{}] wasn't found on {}".format(ami_version, region_name)
    if build_id == 'all':
        return amis
    else:
        return amis[:1]


def tag_ami(ami_id, tags_dict, region_name):
    tags = [{'Key': key, 'Value': value} for key, value in tags_dict.items()]

    ec2 = boto3.resource('ec2', region_name=region_name)
    test_image = ec2.Image(ami_id)
    tags += test_image.tags
    test_image.create_tags(Tags=tags)

    LOGGER.info("tagged %s with %s", ami_id, tags)


def get_non_system_ks_cf_list(loader_node, db_node, request_timeout=300, filter_out_table_with_counter=False,
                              filter_out_mv=False):
    """Get all not system keyspace.tables pairs

    Arguments:
        loader_node {BaseNode} -- LoaderNoder to send request
        db_node_ip {str} -- ip of db_node
    """
    # pylint: disable=too-many-locals

    def get_tables_columns_list(entity_type):
        if entity_type == 'view':
            cmd = "paging off; SELECT keyspace_name, view_name FROM system_schema.views"
        else:
            cmd = "paging off; SELECT keyspace_name, table_name, type FROM system_schema.columns"
        result = loader_node.run_cqlsh(cmd=cmd, timeout=request_timeout, verbose=False, target_db_node=db_node,
                                       split=True, connect_timeout=request_timeout)
        if not result:
            return []

        splitter_result = []
        for row in result[4:]:
            if '|' not in row:
                continue
            if row.startswith('system'):
                continue
            splitter_result.append(row.split('|'))
        return splitter_result

    views_list = set()
    if filter_out_mv:
        tables = get_tables_columns_list('view')

        for table in tables:
            views_list.add('.'.join([name.strip() for name in table[:2]]))
        views_list = list(views_list)

    result = get_tables_columns_list('column')
    if not result:
        return []

    avaialable_ks_cf = defaultdict(list)
    for row in result:
        ks_cf_name = '.'.join([name.strip() for name in row[:2]])

        if filter_out_mv and ks_cf_name in views_list:
            continue

        column_type = row[2].strip()
        avaialable_ks_cf[ks_cf_name].append(column_type)

    if filter_out_table_with_counter:
        for ks_cf, column_types in avaialable_ks_cf.items():
            if 'counter' in column_types:
                avaialable_ks_cf.pop(ks_cf)
    return avaialable_ks_cf.keys()


def remove_files(path):
    LOGGER.debug("Remove path %s", path)
    try:
        if os.path.isdir(path):
            shutil.rmtree(path=path, ignore_errors=True)
        if os.path.isfile(path):
            os.remove(path)
    except Exception as details:  # pylint: disable=broad-except
        LOGGER.error("Error during remove archived logs %s", details)


def format_timestamp(timestamp):
    return datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


def makedirs(path):
    """

    TODO: when move to python3, this function will be replaced
    with os.makedirs function:
        os.makedirs(name, mode=0o777, exist_ok=False)

    """
    try:
        os.makedirs(path)
    except OSError:
        if os.path.exists(path):
            return
        raise


def wait_ami_available(client, ami_id):
    """Wait while ami_id become available

    Wait while ami_id become available, after
    10 minutes return an error

    Arguments:
        client {boto3.EC2.Client} -- client of EC2 service
        ami_id {str} -- ami id to check availability
    """
    waiter = client.get_waiter('image_available')
    waiter.wait(ImageIds=[ami_id],
                WaiterConfig={
                    'Delay': 30,
                    'MaxAttempts': 20}
                )


def update_certificates():
    """
    Update the certificate of server encryption, which might be expired.
    """
    try:
        from sdcm.remote import LocalCmdRunner
        localrunner = LocalCmdRunner()
        localrunner.run('openssl x509 -req -in data_dir/ssl_conf/example/db.csr -CA data_dir/ssl_conf/cadb.pem -CAkey data_dir/ssl_conf/example/cadb.key -CAcreateserial -out data_dir/ssl_conf/db.crt -days 365')
        localrunner.run('openssl x509 -enddate -noout -in data_dir/ssl_conf/db.crt')
    except Exception as ex:
        raise Exception('Failed to update certificates by openssl: %s' % ex)


def s3_download_dir(bucket, path, target):
    """
    Downloads recursively the given S3 path to the target directory.
    :param bucket: the name of the bucket to download from
    :param path: The S3 directory to download.
    :param target: the local directory to download the files to.
    """

    client = boto3.client('s3')

    # Handle missing / at end of prefix
    if not path.endswith('/'):
        path += '/'
    if path.startswith('/'):
        path = path[1:]
    result = client.list_objects_v2(Bucket=bucket, Prefix=path)
    # Download each file individually
    for key in result['Contents']:
        # Calculate relative path
        rel_path = key['Key'][len(path):]
        # Skip paths ending in /
        if not key['Key'].endswith('/'):
            local_file_path = os.path.join(target, rel_path)
            # Make sure directories exist
            local_file_dir = os.path.dirname(local_file_path)
            makedirs(local_file_dir)
            LOGGER.info("Downloading %s from s3 to %s", key['Key'], local_file_path)
            client.download_file(bucket, key['Key'], local_file_path)


def gce_download_dir(bucket, path, target):
    """
    Downloads recursively the given google storage path to the target directory.
    :param bucket: the name of the bucket to download from
    :param path: The google storage directory to download.
    :param target: the local directory to download the files to.
    """

    from sdcm.keystore import KeyStore
    gcp_credentials = KeyStore().get_gcp_credentials()
    gce_driver = libcloud.storage.providers.get_driver(libcloud.storage.types.Provider.GOOGLE_STORAGE)

    driver = gce_driver(gcp_credentials["project_id"] + "@appspot.gserviceaccount.com",
                        gcp_credentials["private_key"],
                        project=gcp_credentials["project_id"])

    if not path.endswith('/'):
        path += '/'
    if path.startswith('/'):
        path = path[1:]

    container = driver.get_container(container_name=bucket)
    dir_listing = driver.list_container_objects(container, ex_prefix=path)
    for obj in dir_listing:
        rel_path = obj.name[len(path):]
        local_file_path = os.path.join(target, rel_path)

        local_file_dir = os.path.dirname(local_file_path)
        makedirs(local_file_dir)
        LOGGER.info("Downloading %s from gcp to %s", obj.name, local_file_path)
        obj.download(destination_path=local_file_path, overwrite_existing=True)


def download_dir_from_cloud(url):
    """
    download a directory from AWS S3 or from google storage

    :param url: a url that starts with `s3://` or `gs://`
    :return: the temp directory create with the downloaded content
    """
    if url is None:
        return url

    md5 = hashlib.md5()
    md5.update(url)
    tmp_dir = os.path.join('/tmp/download_from_cloud', md5.hexdigest())
    parsed = urlparse(url)
    LOGGER.info("Downloading [%s] to [%s]", url, tmp_dir)
    if os.path.isdir(tmp_dir) and os.listdir(tmp_dir):
        LOGGER.warning("[{}] already exists, skipping download".format(tmp_dir))
    else:
        if url.startswith('s3://'):
            s3_download_dir(parsed.hostname, parsed.path, tmp_dir)
        elif url.startswith('gs://'):
            gce_download_dir(parsed.hostname, parsed.path, tmp_dir)
        elif os.path.isdir(url):
            tmp_dir = url
        else:
            raise ValueError("Unsupported url schema or non-existing directory [{}]".format(url))
    if not tmp_dir.endswith('/'):
        tmp_dir += '/'
    LOGGER.info("Finished downloading [%s]", url)
    return tmp_dir


def filter_aws_instances_by_type(instances):
    filtered_instances = {
        "db_nodes": [],
        "loader_nodes": [],
        "monitor_nodes": []
    }

    for instance in instances:
        name = [tag['Value']
                for tag in instance['Tags'] if tag['Key'] == 'Name']
        if 'db-node' in name[0]:
            filtered_instances["db_nodes"].append(instance)
        if 'monitor-node' in name[0]:
            filtered_instances["monitor_nodes"].append(instance)
        if 'loader-node' in name[0]:
            filtered_instances["loader_nodes"].append(instance)
    return filtered_instances


def filter_gce_instances_by_type(instances):
    filtered_instances = {
        "db_nodes": [],
        "loader_nodes": [],
        "monitor_nodes": []
    }

    for instance in instances:
        if 'db-nodes' in instance.name:
            filtered_instances["db_nodes"].append(instance)
        if 'monitor-node' in instance.name:
            filtered_instances["monitor_nodes"].append(instance)
        if 'loader-node' in instance.name:
            filtered_instances["loader_nodes"].append(instance)
    return filtered_instances


BUILDERS = [
    {
        "name": "aws-scylla-qa-builder3",
        "public_ip": "18.235.64.163",
        "user": "jenkins",
        "key_file": os.path.expanduser("~/.ssh/scylla-qa-ec2")
    },
    {
        "name": "aws-eu-west1-qa-builder1",
        "public_ip": "18.203.132.87",
        "user": "jenkins",
        "key_file": os.path.expanduser("~/.ssh/scylla-qa-ec2")
    },
    {
        "name": "aws-eu-west1-qa-builder2",
        "public_ip": "34.244.95.165",
        "user": "jenkins",
        "key_file": os.path.expanduser("~/.ssh/scylla-qa-ec2")
    },
    {
        "name": "aws-eu-west1-qa-builder4",
        "public_ip": "34.253.184.117",
        "user": "jenkins",
        "key_file": os.path.expanduser("~/.ssh/scylla-qa-ec2")
    },
    {
        "name": "aws-eu-west1-qa-builder4",
        "public_ip": "52.211.130.106",
        "user": "jenkins",
        "key_file": os.path.expanduser("~/.ssh/scylla-qa-ec2")
    }
]


def get_builder_by_test_id(test_id):
    from sdcm.remote import RemoteCmdRunner

    base_path_on_builder = "/home/jenkins/slave/workspace"
    found_builders = []

    def search_test_id_on_builder(builder):
        remoter = RemoteCmdRunner(builder['public_ip'],
                                  user=builder['user'],
                                  key_file=builder['key_file'])
        LOGGER.info('Search on %s', builder['name'])
        result = remoter.run("find {where} -name test_id | xargs grep -rl {test_id}".format(where=base_path_on_builder,
                                                                                            test_id=test_id),
                             ignore_status=True, verbose=False)

        if not result.exited and not result.stderr:
            path = result.stdout.strip()
            LOGGER.info("Builder name %s, ip %s, folder %s", builder['name'], builder['public_ip'], path)
            return {"builder": builder, "path": os.path.dirname(path)}
        else:
            LOGGER.info("Nothing found")
            return None

    search_obj = ParallelObject(BUILDERS, timeout=30, num_workers=len(BUILDERS))
    results = search_obj.run(search_test_id_on_builder)
    found_builders = [builder for builder in results if builder]
    if not found_builders:
        LOGGER.info("Nothing found for %s", test_id)

    return found_builders


def get_post_behavior_actions(config):
    action_per_type = {
        "db_nodes": None,
        "monitor_nodes": None,
        "loader_nodes": None
    }

    for key in action_per_type:
        config_key = 'post_behavior_{}'.format(key)
        old_config_key = config.get('failure_post_behavior', 'destroy')
        action_per_type[key] = config.get(config_key, old_config_key)

    return action_per_type


def clean_aws_instances_according_post_behavior(params, config, logdir):  # pylint: disable=invalid-name

    status = get_testrun_status(params.get('TestId'), logdir)

    def apply_action(instances, action):
        if action == 'destroy':
            instances_ids = [instance['InstanceId'] for instance in instances]
            LOGGER.info('Clean next instances %s', instances_ids)
            client.terminate_instances(InstanceIds=instances_ids)
        elif action == 'keep-on-failure':
            if status:
                LOGGER.info('Run failed. Leave instances running')
            else:
                LOGGER.info('Run was Successful. Killing nodes')
                apply_action(instances, action='destroy')
        elif action == 'keep':
            LOGGER.info('Leave instances running')
        else:
            LOGGER.warning('Unsupported action %s', action)

    aws_instances = list_instances_aws(params, group_as_region=True)
    for region, instances in aws_instances.items():
        if not instances:
            continue
        client = boto3.client("ec2", region_name=region)
        filtered_instances = filter_aws_instances_by_type(instances)
        actions_per_type = get_post_behavior_actions(config)

        for instance_set_type, action in actions_per_type.items():
            LOGGER.info('Apply action "%s" for %s instances', action, instance_set_type)
            apply_action(filtered_instances[instance_set_type], action)


def clean_gce_instances_according_post_behavior(params, config, logdir):  # pylint: disable=invalid-name

    status = get_testrun_status(params.get('TestId'), logdir)

    def apply_action(instances, action):
        if action == 'destroy':
            for instance in filtered_instances['db_nodes']:
                LOGGER.info('Destroying instance: %s', instance.name)
                instance.destroy()
                LOGGER.info('Destroyed instance: %s', instance.name)
        elif action == 'keep-on-failure':
            if status:
                LOGGER.info('Run failed. Leave instances running')
            else:
                LOGGER.info('Run wasSuccessful. Killing nodes')
                apply_action(instances, action='destroy')
        elif action == 'keep':
            LOGGER.info('Leave instances runing')
        else:
            LOGGER.warning('Unsupported action %s', action)

    gce_instances = list_instances_gce(params)
    filtered_instances = filter_gce_instances_by_type(gce_instances)
    actions_per_type = get_post_behavior_actions(config)

    for instance_set_type, action in actions_per_type.items():
        apply_action(filtered_instances[instance_set_type], action)


def search_test_id_in_latest(logdir):
    from sdcm.remote import LocalCmdRunner

    test_id = None
    result = LocalCmdRunner().run('cat {0}/latest/test_id'.format(logdir), ignore_status=True)
    if not result.exited and result.stdout:
        test_id = result.stdout.strip()
        LOGGER.info("Found latest test_id: {}".format(test_id))
        LOGGER.info("Collect logs for test-run with test-id: {}".format(test_id))
    else:
        LOGGER.error('test_id not found. Exit code: %s; Error details %s', result.exited, result.stderr)
    return test_id


def get_testrun_dir(base_dir, test_id=None):
    from sdcm.remote import LocalCmdRunner

    if not test_id:
        test_id = search_test_id_in_latest(base_dir)
    LOGGER.info('Search dir with logs locally for test id: %s', test_id)
    search_cmd = "find {base_dir} -name test_id | xargs grep -rl {test_id}".format(**locals())
    result = LocalCmdRunner().run(cmd=search_cmd, ignore_status=True)
    LOGGER.info("Search result %s", result)
    if result.exited == 0 and result.stdout:
        found_dirs = result.stdout.strip().split('\n')
        LOGGER.info(found_dirs)
        return os.path.dirname(found_dirs[0])
    LOGGER.info("No any dirs found locally for current test id")
    return None


def get_testrun_status(test_id=None, logdir=None):

    testrun_dir = get_testrun_dir(logdir, test_id)
    status = None
    if testrun_dir:
        with open(os.path.join(testrun_dir, 'events_log/critical.log')) as f:  # pylint: disable=invalid-name
            status = f.read().split()

    return status
