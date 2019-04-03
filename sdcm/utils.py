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

import os
import logging
import time
import datetime
import requests
from functools import wraps
from enum import Enum
from collections import defaultdict

import boto3
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider

logger = logging.getLogger('avocado.test')


def _remote_get_hash(remoter, file_path):
    try:
        result = remoter.run('md5sum {}'.format(file_path), verbose=True)
        return result.stdout.strip().split()[0]
    except Exception as details:
        test_logger = logging.getLogger('avocado.test')
        test_logger.error(str(details))
        return None


def _remote_get_file(remoter, src, dst, user_agent=None):
    cmd = 'curl -L {} -o {}'.format(src, dst)
    if user_agent:
        cmd += ' --user-agent %s' % user_agent
    result = remoter.run(cmd, ignore_status=True)


def remote_get_file(remoter, src, dst, hash_expected=None, retries=1, user_agent=None):
    _remote_get_file(remoter, src, dst, user_agent)
    if not hash_expected:
        return
    while retries > 0 and _remote_get_hash(remoter, dst) != hash_expected:
        _remote_get_file(remoter, src, dst, user_agent)
        retries -= 1
    #assert _remote_get_hash(remoter, dst) == hash_expected


class retrying(object):
    """
        Used as a decorator to retry function run that can possibly fail with allowed exceptions list
    """

    def __init__(self, n=3, sleep_time=1, allowed_exceptions=(Exception,), message=""):
        self.n = n  # number of times to retry
        self.sleep_time = sleep_time  # number seconds to sleep between retries
        self.allowed_exceptions = allowed_exceptions  # if Exception is not allowed will raise
        self.message = message  # string that will be printed between retries

    def __call__(self, func):
        @wraps(func)
        def inner(*args, **kwargs):
            for i in xrange(self.n):
                try:
                    if self.message:
                        logger.info("%s [try #%s]" % (self.message, i))
                    return func(*args, **kwargs)
                except self.allowed_exceptions as e:
                    logger.debug("retrying: %r" % e)
                    time.sleep(self.sleep_time)
                    if i == self.n - 1:
                        logger.error("Number of retries exceeded!")
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
            if len(args) > 0 and func.__name__ in dir(args[0]):
                class_name = " <%s>" % args[0].__class__.__name__
            action = "%s%s" % (msg, class_name)
            start_time = datetime.datetime.now()
            logger.debug("BEGIN: %s", action)
            res = func(*args, **kwargs)
            end_time = datetime.datetime.now()
            logger.debug("END: %s (ran %ss)", action, (end_time - start_time).total_seconds())
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
    DEBIAN8 = 5
    DEBIAN9 = 6


def get_data_dir_path(*args):
    import sdcm
    sdcm_path = os.path.realpath(sdcm.__path__[0])
    data_dir = os.path.join(sdcm_path, "../data_dir", *args)
    return os.path.abspath(data_dir)


def get_job_name():
    return os.environ.get('JOB_NAME', 'local_run')


def verify_scylla_repo_file(content, is_rhel_like=True):
    logger.info('Verifying Scylla repo file')
    if is_rhel_like:
        body_prefix = ['#', '[scylla', 'name=', 'baseurl=', 'enabled=', 'gpgcheck=', 'type=',
                       'skip_if_unavailable=', 'gpgkey=', 'repo_gpgcheck=', 'enabled_metadata=']
    else:
        body_prefix = ['#', 'deb']
    for line in content.split('\n'):
        valid_prefix = False
        for prefix in body_prefix:
            if line.startswith(prefix) or len(line.strip()) == 0:
                valid_prefix = True
                break
        logger.debug(line)
        assert valid_prefix, 'Repository content has invalid line: {}'.format(line)


def remove_comments(data):
    """Remove comments line from data

    Remove any string which is start from # in data

    Arguments:
        data {str} -- data expected the command output, file contents
    """
    return '\n'.join([i.strip() for i in data.split('\n') if not i.startswith('#')])


class S3Storage(object):
    @staticmethod
    def generate_url(file_path):
        job_name = get_job_name()
        file_name = os.path.basename(os.path.normpath(file_path))
        return "https://cloudius-jenkins-test.s3.amazonaws.com/{job_name}/{file_name}".format(**locals())

    @classmethod
    def upload_file(cls, file_path):
        try:
            s3_url = cls.generate_url(file_path)
            with open(file_path) as fh:
                mydata = fh.read()
                logger.info("Uploading '{file_path}' to {s3_url}".format(**locals()))
                response = requests.put(s3_url, data=mydata)
                logger.debug(response)
                return s3_url if response.ok else ""
        except Exception as e:
            logger.debug("Unable to upload to S3: %s" % e)
            return ""


aws_regions = ['us-east-1', 'us-west-2', 'eu-west-1']
gce_regions = ['us-east1-b', 'us-west1-b', 'us-east4-b']


def clean_cloud_instances(tags_dict):
    """
    Remove all instances with specific tags from both AWS/GCE

    :param tags_dict: a dict of the tag to select the instances, e.x. {"TestId": "9bc6879f-b1ef-47e1-99ab-020810aedbcc"}
    :return: None
    """
    clean_instances_aws(tags_dict)
    clean_instances_gce(tags_dict)


def clean_instances_aws(tags_dict):
    """
    Remove all instances with specific tags AWS

    :param tags_dict: a dict of the tag to select the instances, e.x. {"TestId": "9bc6879f-b1ef-47e1-99ab-020810aedbcc"}
    :return: None
    """
    for region in aws_regions:
        logger.info('going to cleanup aws region=%s', region)
        client = boto3.client('ec2', region_name=region)
        custom_filter = [{'Name': 'tag:{}'.format(key), 'Values': [value]} for key, value in tags_dict.items()]

        response = client.describe_instances(Filters=custom_filter)
        instance_ids = [instance['InstanceId'] for reservation in response['Reservations'] for instance in reservation['Instances']]

        if instance_ids:
            logger.info("going to delete instance_ids={}".format(instance_ids))
            response = client.terminate_instances(InstanceIds=instance_ids)
            for instance in response['TerminatingInstances']:
                assert instance['CurrentState']['Name'] in ['terminated', 'shutting-down']


def clean_instances_gce(tags_dict):
    """
    Remove all instances with specific tags GCE

    :param tags_dict: a dict of the tag to select the instances, e.x. {"TestId": "9bc6879f-b1ef-47e1-99ab-020810aedbcc"}
    :return: None
    """

    # avoid cyclic dependency issues, since too many things import utils.py
    from .keystore import KeyStore

    gcp_credentials = KeyStore().get_gcp_credentials()
    compute_engine = get_driver(Provider.GCE)
    for region in gce_regions:
        logger.info('going to cleanup gce region=%s', region)
        driver = compute_engine(gcp_credentials["project_id"] + "@appspot.gserviceaccount.com",
                                gcp_credentials["private_key"],
                                datacenter=region,
                                project=gcp_credentials["project_id"])

        for node in driver.list_nodes():
            tags = node.extra['metadata'].get('items', [])
            # since libcloud list_nodes() doesn't offer any filtering
            # we go over all the expected tags, and check if they exist in on the node/instance
            # if all of them exists we delete that node
            if all([any([t['key'] == key and t['value'] == value for t in tags]) for key, value in tags_dict.items()]):
                if not node.state == 'STOPPED':
                    logger.info("going to delete: {}".format(node.name))
                    res = driver.destroy_node(node)
                    logger.info("{} deleted. res={}".format(node.name, res))


def list_instances_aws(tags_dict, region_name=None):
    """
    list all instances with specific tags AWS

    :param tags_dict: a dict of the tag to select the instances, e.x. {"TestId": "9bc6879f-b1ef-47e1-99ab-020810aedbcc"}
    :param region_name: name of the region to list

    :return: None
    """
    instances = []
    _aws_regions = [region_name] if region_name else aws_regions
    for region in _aws_regions:
        logger.info('going to list aws region=%s', region)
        client = boto3.client('ec2', region_name=region)
        custom_filter = [{'Name': 'tag:{}'.format(key), 'Values': [value]} for key, value in tags_dict.items()]

        response = client.describe_instances(Filters=custom_filter)
        instances += [instance for reservation in response['Reservations'] for instance in reservation['Instances']]

    return instances


def list_instances_gce(tags_dict, region_name=None):
    """
    list all instances with specific tags GCE

    :param tags_dict: a dict of the tag to select the instances, e.x. {"TestId": "9bc6879f-b1ef-47e1-99ab-020810aedbcc"}
    :param region_name: name of the region to list

    :return: None
    """

    # avoid cyclic dependency issues, since too many things import utils.py
    from .keystore import KeyStore

    instances = []
    gcp_credentials = KeyStore().get_gcp_credentials()
    compute_engine = get_driver(Provider.GCE)

    _gce_regions = [region_name] if region_name else gce_regions
    for region in _gce_regions:
        logger.info('going to list gce region=%s', region)
        driver = compute_engine(gcp_credentials["project_id"] + "@appspot.gserviceaccount.com",
                                gcp_credentials["private_key"],
                                datacenter=region,
                                project=gcp_credentials["project_id"])

        for node in driver.list_nodes():
            tags = node.extra['metadata'].get('items', [])
            # since libcloud list_nodes() doesn't offer any filtering
            # we go over all the expected tags, and check if they exist in on the node/instance
            # if all of them exists we delete that node
            if all([any([t['key'] == key and t['value'] == value for t in tags]) for key, value in tags_dict.items()]):
                if not node.state == 'STOPPED':
                    instances += [node]
    return instances


_scylla_ami_cache = None


def get_scylla_ami_versions(region):
    """
    get the list of all the formal scylla ami from specific region

    :param region: the aws region to look in
    :return: list of ami data
    :rtype: list
    """
    global _scylla_ami_cache

    if _scylla_ami_cache:
        return _scylla_ami_cache

    EC2 = boto3.client('ec2', region_name=region)
    response = EC2.describe_images(
        Owners=['797456418907'],  # ScyllaDB
        Filters=[
            {'Name': 'name', 'Values': ['ScyllaDB *']},
        ],
    )

    _scylla_ami_cache = sorted(response['Images'],
                               key=lambda x: x['CreationDate'],
                               reverse=True)

    return _scylla_ami_cache


_s3_scylla_repos_cache = defaultdict(dict)


def get_s3_scylla_repos_mapping(dist_type='centos', dist_version=None):
    """
    get the mapping from version prefixes to rpm .repo or deb .list files locations

    :param dist_type: which distro to look up centos/ubuntu/debian
    :param dist_version: famaily name of the distro version

    :return: a mapping of versions prefixes to repos
    :rtype: dict
    """
    if (dist_type, dist_version) in _s3_scylla_repos_cache:
        return _s3_scylla_repos_cache[(dist_type, dist_version)]

    s3_client = boto3.client('s3')
    bucket = 'downloads.scylladb.com'

    if dist_type == 'centos':
        response = s3_client.list_objects(Bucket=bucket, Prefix='rpm/centos/',  Delimiter='/')

        for f in response['Contents']:
            filename = os.path.basename(f['Key'])
            # only if path look like 'rpm/centos/scylla-1.3.repo', we deem it formal one
            if filename.startswith('scylla-') and filename.endswith('.repo'):
                version_prefix = filename.replace('.repo', '').split('-')[-1]
                _s3_scylla_repos_cache[(dist_type, dist_version)][version_prefix] = "https://s3.amazonaws.com/{bucket}/{path}".format(bucket=bucket, path=f['Key'])

    elif dist_type == 'ubuntu' or dist_type == 'debian':
        response = s3_client.list_objects(Bucket=bucket, Prefix='deb/{}/'.format(dist_type),  Delimiter='/')
        for f in response['Contents']:
            filename = os.path.basename(f['Key'])

            # only if path look like 'deb/debian/scylla-3.0-jessie.list', we deem it formal one
            if filename.startswith('scylla-') and filename.endswith('-{}.list'.format(dist_version)):

                version_prefix = filename.replace('-{}.list'.format(dist_version), '').split('-')[-1]
                _s3_scylla_repos_cache[(dist_type, dist_version)][version_prefix] = "https://s3.amazonaws.com/{bucket}/{path}".format(bucket=bucket, path=f['Key'])

    else:
        raise NotImplementedError("[{}] is not yet supported".format(dist_type))
    return _s3_scylla_repos_cache[(dist_type, dist_version)]
