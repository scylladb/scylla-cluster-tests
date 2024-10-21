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

from __future__ import absolute_import, annotations

import atexit
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
import copy
import string
import warnings
import getpass
import re
import uuid
import zipfile
import io
import tempfile
import traceback
import ctypes
import shlex
from typing import Iterable, List, Callable, Optional, Dict, Union, Literal, Any, Type
from urllib.parse import urlparse
from unittest.mock import Mock
from textwrap import dedent
from contextlib import closing, contextmanager
from functools import wraps, cached_property, lru_cache, singledispatch
from collections import defaultdict, namedtuple
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from concurrent.futures.thread import _python_exit
import hashlib
from pathlib import Path
from collections import OrderedDict
import requests
import boto3
from invoke import UnexpectedExit
from mypy_boto3_s3 import S3Client, S3ServiceResource
from mypy_boto3_ec2 import EC2Client, EC2ServiceResource
from mypy_boto3_ec2.service_resource import Image as EC2Image
import docker  # pylint: disable=wrong-import-order; false warning because of docker import (local file vs. package)
from google.cloud.storage import Blob as GceBlob
from google.cloud.compute_v1.types import Metadata as GceMetadata, Instance as GceInstance
from google.cloud.compute_v1 import ListImagesRequest, Image as GceImage
from packaging.version import Version
from prettytable import PrettyTable

from sdcm.remote.libssh2_client import UnexpectedExit as Libssh2_UnexpectedExit
from sdcm.sct_events import Severity
from sdcm.sct_events.system import CpuNotHighEnoughEvent, SoftTimeoutEvent
from sdcm.utils.aws_utils import (
    AwsArchType,
    EksClusterForCleaner,
    get_scylla_images_ec2_resource,
    get_ssm_ami,
    get_by_owner_ami,
)
from sdcm.utils.ssh_agent import SSHAgent
from sdcm.utils.decorators import retrying
from sdcm import wait
from sdcm.utils.ldap import DEFAULT_PWD_SUFFIX, SASLAUTHD_AUTHENTICATOR, LdapServerType
from sdcm.keystore import KeyStore
from sdcm.utils.gce_utils import (
    GkeCleaner,
    gce_public_addresses,
)
from sdcm.remote import LocalCmdRunner
from sdcm.remote import RemoteCmdRunnerBase
from sdcm.utils.gce_utils import (
    get_gce_compute_instances_client,
    get_gce_compute_images_client,
    get_gce_compute_addresses_client,
    get_gce_compute_regions_client,
    get_gce_storage_client,
)


LOGGER = logging.getLogger('utils')
DEFAULT_AWS_REGION = "eu-west-1"
DOCKER_CGROUP_RE = re.compile("/docker/([0-9a-f]+)")
SCYLLA_AMI_OWNER_ID_LIST = ["797456418907", "158855661827"]
SCYLLA_GCE_IMAGES_PROJECT = "scylla-images"


class KeyBasedLock():  # pylint: disable=too-few-public-methods
    """Class designed for creating locks based on hashable keys."""

    def __init__(self):
        self.key_lock_mapping = {}
        self.handler_lock = threading.Lock()

    def get_lock(self, hashable_key):
        with self.handler_lock:
            if hashable_key not in self.key_lock_mapping:
                self.key_lock_mapping[hashable_key] = threading.Lock()
            return self.key_lock_mapping[hashable_key]


def deprecation(message):
    warnings.warn(message, DeprecationWarning, stacklevel=3)


def _remote_get_hash(remoter, file_path):
    try:
        result = remoter.run('md5sum {}'.format(file_path), verbose=True)
        return result.stdout.strip().split()[0]
    except Exception as details:  # pylint: disable=broad-except  # noqa: BLE001
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


def get_first_view_with_name_like(view_name_substr: str, session) -> tuple:
    query = f"select keyspace_name, view_name, base_table_name from system_schema.views " \
            f"where view_name like '%_{view_name_substr}' ALLOW FILTERING"
    LOGGER.debug("Run query: %s", query)
    result = session.execute(query)
    if not result:
        return None, None, None

    return result.one().keyspace_name, result.one().view_name, result.one().base_table_name


def get_views_of_base_table(keyspace_name: str, base_table_name: str, session) -> list:
    query = f"select view_name from system_schema.views " \
            f"where keyspace_name = '{keyspace_name}' and base_table_name = '{base_table_name}' ALLOW FILTERING"
    LOGGER.debug("Run query: %s", query)
    result = session.execute(query)
    views = []
    for row in result:
        views.append(row.view_name)

    return views


def get_entity_columns(keyspace_name: str, entity_name: str, session) -> list:
    query = f"select column_name, kind, type from system_schema.columns where keyspace_name = '{keyspace_name}' " \
            f"and table_name='{entity_name}'"
    LOGGER.debug("Run query: %s", query)
    result = session.execute(query)
    view_details = []

    for row in result:
        view_details.append({"column_name": row.column_name, "kind": row.kind, "type": row.type})

    return view_details


def generate_random_string(length):
    return random.choice(string.ascii_uppercase) + ''.join(
        random.choice(string.ascii_uppercase + string.digits) for x in range(length - 1))


def str_to_bool(bool_as_str: str) -> bool:
    if isinstance(bool_as_str, bool):
        return bool_as_str
    elif isinstance(bool_as_str, str):
        return bool_as_str.lower() in ("true", "yes", "y", "1")
    elif bool_as_str is None:
        return False
    raise ValueError("'bool_as_str' is of the unexpected type: %s" % type(bool_as_str))


def get_data_dir_path(*args):
    sct_root_path = get_sct_root_path()
    data_dir = os.path.join(sct_root_path, "data_dir", *args)
    return os.path.abspath(data_dir)


def get_sct_root_path():
    import sdcm  # pylint: disable=import-outside-toplevel
    sdcm_path = os.path.realpath(sdcm.__path__[0])
    sct_root_dir = os.path.join(sdcm_path, "..")
    return os.path.abspath(sct_root_dir)


def find_file_under_sct_dir(filename: str, sub_folder: str = None):
    """
    Find file under scylla_cluster_test where sub_folder is part of the file path (optional)
    """
    sct_path = Path(get_sct_root_path())
    profile_file_full_path = sct_path.rglob(filename)
    for profile_file_path in profile_file_full_path:
        # sub_folder == "/tmp" - cover user profile path inside cassandra-stress command, like:
        # "cassandra-stress user profile=/tmp/c-s_lwt_basic.yaml ops'(select=1)'
        if sub_folder == "/tmp" or sub_folder is None:
            # return first found file with the name
            return str(profile_file_path)
        elif sub_folder in str(profile_file_path.parent):
            return str(profile_file_path)

    raise FileNotFoundError(f"User profile file {filename} not found under {str(sct_path)}")


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


class S3Storage():
    bucket_name = 'cloudius-jenkins-test'
    enable_multipart_threshold_size = 1024 * 1024 * 1024  # 1GB
    multipart_chunksize = 50 * 1024 * 1024  # 50 MB
    num_download_attempts = 5

    def __init__(self, bucket=None):
        if bucket:
            self.bucket_name = bucket
        self._bucket: S3ServiceResource.Bucket = boto3.resource("s3").Bucket(name=self.bucket_name)
        self.transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=self.enable_multipart_threshold_size,
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
        s3_obj = "{}/{}".format(dest_dir, os.path.basename(file_path))
        try:
            LOGGER.info("Uploading '{file_path}' to {s3_url}".format(file_path=file_path, s3_url=s3_url))
            self._bucket.upload_file(Filename=file_path,
                                     Key=s3_obj,
                                     Config=self.transfer_config)
            LOGGER.info("Uploaded to {0}".format(s3_url))
            LOGGER.info("Set public read access")
            self.set_public_access(key=s3_obj)
            return s3_url
        except Exception as details:  # pylint: disable=broad-except  # noqa: BLE001
            LOGGER.debug("Unable to upload to S3: %s", details)
            return ""

    def set_public_access(self, key):
        acl_obj: S3ServiceResource = boto3.resource('s3').ObjectAcl(self.bucket_name, key)

        grants = copy.deepcopy(acl_obj.grants)
        grantees = {
            'Grantee': {
                "Type": "Group",
                "URI": "http://acs.amazonaws.com/groups/global/AllUsers"
            },
            'Permission': "READ"
        }
        grants.append(grantees)
        acl_obj.put(ACL='', AccessControlPolicy={'Grants': grants, 'Owner': acl_obj.owner})

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

        except Exception as details:  # pylint: disable=broad-except  # noqa: BLE001
            LOGGER.warning("File {} is not downloaded by reason: {}".format(key_name, details))
            return ""


def list_logs_by_test_id(test_id):
    log_types = ['db-cluster', 'monitor-set', 'loader-set', 'event', 'sct', 'jepsen-data', 'siren-manager-set',
                 'prometheus', 'grafana', 'kubernetes', 'job', 'monitoring_data_stack', 'output', 'error',
                 'summary', 'warning', 'critical', 'normal', 'debug', 'left_processes', 'email_data', 'corrupted-sstables',
                 'sstables']

    results = []

    if not test_id:
        return results

    def convert_to_date(date_str):
        time_formats = [
            "%Y%m%d_%H%M%S",
            "%Y_%m_%d_%H_%M_%S",
        ]
        for time_format in time_formats:
            try:
                return datetime.datetime.strptime(date_str, time_format)
            except ValueError:
                continue
        # for old data collected or uploaded without datetime,
        # return the old date to display them earlier
        return datetime.datetime(2019, 1, 1, 1, 1, 1)

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


def list_parallel_timelines_report_urls(test_id: str) -> list[str | None]:
    name_to_search = 'parallel-timelines-report'
    available_logs_paths = S3Storage().search_by_path(path=test_id)
    report_path_list = [log_file_path for log_file_path in available_logs_paths if name_to_search in log_file_path]
    LOGGER.debug("Found saved report files:\n%s", ", ".join(report_path_list))
    # log_file_path looks like
    # 88605a0b-aa5a-4da9-bb58-5dd2a94c5350/20220109_092346/parallel-timelines-report-88605a0b.tar.gz
    # Perform reverse order sorting by date inside this path
    report_path_list.sort(key=lambda x: x.split('/')[1], reverse=True)
    return [f"https://{S3Storage.bucket_name}.s3.amazonaws.com/{report_path}" for report_path in report_path_list]


def all_aws_regions(cached=False):
    if cached:
        return [
            'eu-north-1',
            'ap-south-1',
            'eu-west-3',
            'eu-west-2',
            'eu-west-1',
            'ap-northeast-2',
            'ap-northeast-1',
            'sa-east-1',
            'ca-central-1',
            'ap-southeast-1',
            'ap-southeast-2',
            'eu-central-1',
            'us-east-1',
            'us-east-2',
            'us-west-1',
            'us-west-2'
        ]
    else:
        client: EC2Client = boto3.client('ec2', region_name=DEFAULT_AWS_REGION)
        return [region['RegionName'] for region in client.describe_regions()['Regions']]


class ParallelObject:
    """
        Run function in with supplied args in parallel using thread.
    """

    def __init__(self, objects: Iterable, timeout: int = 6,  # pylint: disable=redefined-outer-name
                 num_workers: int = None, disable_logging: bool = False):
        """Constructor for ParallelObject

        Build instances of Parallel object. Item of objects is used as parameter for
        disrupt_func which will be run in parallel.

        :param objects: if item in object is list, it will be upacked to disrupt_func argument, ex *arg
                if item in object is dict, it will be upacked to disrupt_func keyword argument, ex **kwarg
                if item in object is any other type, will be passed to disrupt_func as is.
                if function accept list as parameter, the item shuld be list of list item = [[]]

        :param timeout: global timeout for running all
        :param num_workers: num of parallel threads, defaults to None
        :param disable_logging: disable logging for running disrupt_func, defaults to False
        """
        self.objects = objects
        self.timeout = timeout
        self.num_workers = num_workers
        self.disable_logging = disable_logging
        self._thread_pool = ThreadPoolExecutor(max_workers=self.num_workers)  # pylint: disable=consider-using-with

    def run(self, func: Callable, ignore_exceptions=False, unpack_objects: bool = False) -> List[ParallelObjectResult]:
        """Run callable object "disrupt_func" in parallel

        Allow to run callable object in parallel.
        if ignore_exceptions is true,  return
        list of FutureResult object instances which contains
        two attributes:
            - result - result of callable object execution
            - exc - exception object, if happened during run
        if ignore_exceptions is False, then running will
        terminated on future where happened exception or by timeout
        what has stepped first.

        :param func: Callable object to run in parallel
        :param ignore_exceptions: ignore exception and return result, defaults to False
        :param unpack_objects: set to True when unpacking of objects to the disrupt_func as args or kwargs needed
        :returns: list of FutureResult object
        :rtype: {List[FutureResult]}
        """

        def func_wrap(fun):
            @wraps(fun)
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

        results = []

        if not self.disable_logging:
            LOGGER.debug("Executing in parallel: '{}' on {}".format(func.__name__, self.objects))
            func = func_wrap(func)

        futures = []

        for obj in self.objects:
            if unpack_objects and isinstance(obj, (list, tuple)):
                futures.append((self._thread_pool.submit(func, *obj), obj))
            elif unpack_objects and isinstance(obj, dict):
                futures.append((self._thread_pool.submit(func, **obj), obj))
            else:
                futures.append((self._thread_pool.submit(func, obj), obj))
        time_out = self.timeout
        for future, target_obj in futures:
            try:
                result = future.result(time_out)
            except FuturesTimeoutError as exception:
                results.append(ParallelObjectResult(obj=target_obj, exc=exception, result=None))
                time_out = 0.001  # if there was a timeout on one of the futures there is no need to wait for all
            except Exception as exception:  # pylint: disable=broad-except  # noqa: BLE001
                results.append(ParallelObjectResult(obj=target_obj, exc=exception, result=None))
            else:
                results.append(ParallelObjectResult(obj=target_obj, exc=None, result=result))

        self.clean_up(futures)

        if ignore_exceptions:
            return results

        runs_that_finished_with_exception = [res for res in results if res.exc]
        if runs_that_finished_with_exception:
            raise ParallelObjectException(results=results)
        return results

    def call_objects(self, ignore_exceptions: bool = False) -> list["ParallelObjectResult"]:
        """
        Use the ParallelObject run() method to call a list of
        callables in parallel. Rather than running a single function
        with a number of objects as arguments in parallel, we're
        calling a list of callables in parallel.

        If we need to run multiple callables with some arguments, one
        solution is to use partial objects to pack the callable with
        its arguments, e.g.:

        partial_func_1 = partial(print, "lorem")
        partial_func_2 = partial(sum, (2, 3))
        ParallelObject(objects=[partial_func_1, partial_func_2]).call_objects()

        This can be useful if we need to tightly synchronise the
        execution of multiple functions.
        """
        return self.run(lambda x: x(), ignore_exceptions=ignore_exceptions)

    def clean_up(self, futures):
        # if there are futures that didn't run  we cancel them
        for future, _ in futures:
            future.cancel()
        self._thread_pool.shutdown(wait=False)
        # we need to unregister internal function that waits for all threads to finish when interpreter exits
        atexit.unregister(_python_exit)

    @staticmethod
    def run_named_tasks_in_parallel(tasks: dict[str, Callable],
                                    timeout: int,
                                    ignore_exceptions: bool = False) -> dict[str, ParallelObjectResult]:
        """
        Allows calling multiple Callables in parallel using Parallel
        Object. Returns a dict with the results. Will raise an exception
        if:
        - ignore_exceptions is set to False and an exception was raised
        during execution
        - timeout is set and timeout was reached

        Example:

        Given:
        tasks = {
            "trigger": partial(time.sleep, 10))
            "interrupt": partial(random.random)
        }

        Result:

        {
            "trigger": ParallelObjectResult >>> time.sleep result
            "interrupt": ParallelObjectResult >>> random.random result
        }
        """
        task_id_map = {str(id(task)): task_name for task_name, task in tasks.items()}
        results_map = {}

        task_results = ParallelObject(
            objects=tasks.values(),
            timeout=timeout if timeout else None
        ).call_objects(ignore_exceptions=ignore_exceptions)

        for result in task_results:
            task_name = task_id_map.get(str(id(result.obj)))
            results_map.update({task_name: result})

        return results_map


class ParallelObjectResult:  # pylint: disable=too-few-public-methods
    """Object for result of future in ParallelObject

    Return as a result of ParallelObject.run method
    and contain result of disrupt_func was run in parallel
    and exception if it happened during run.
    """

    def __init__(self, obj, result=None, exc=None):
        self.obj = obj
        self.result = result
        self.exc = exc


class ParallelObjectException(Exception):
    def __init__(self, results: List[ParallelObjectResult]):
        super().__init__()
        self.results = results

    def __str__(self):
        ex_str = ""
        for res in self.results:
            if res.exc:
                ex_str += f"{res.obj}:\n {''.join(traceback.format_exception(type(res.exc), res.exc, res.exc.__traceback__))}"
        return ex_str


def docker_current_container_id() -> Optional[str]:
    with open("/proc/1/cgroup", encoding="utf-8") as cgroup:
        for line in cgroup:
            match = DOCKER_CGROUP_RE.search(line)
            if match:
                return match.group(1)
    return None


def list_clients_docker(builder_name: Optional[str] = None, verbose: bool = False) -> Dict[str, docker.DockerClient]:
    log = LOGGER if verbose else Mock()
    docker_clients = {}

    def get_builder_docker_client(builder: Dict[str, str]) -> None:
        if not can_connect_to(builder["public_ip"], 22, timeout=5):
            log.error("%(name)s: can't establish connection to %(public_ip)s:22, port is closed", builder)
            return
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                # since a bug in docker package https://github.com/docker-library/python/issues/517 that need
                # to explicitly pass down the port for supporting ipv6
                client = docker.DockerClient(
                    base_url=f"ssh://{builder['user']}@{normalize_ipv6_url(builder['public_ip'])}:22")
            client.ping()
            log.info("%(name)s: connected via SSH (%(user)s@%(public_ip)s)", builder)
        except Exception:  # pylint: disable=broad-except
            log.error("%(name)s: failed to connect to Docker via SSH", builder)
            raise
        docker_clients[builder["name"]] = client

    if builder_name is None or builder_name == "local":
        docker_clients["local"] = docker.from_env()

    if builder_name != "local" and getpass.getuser() != "jenkins":
        builders = [item["builder"] for item in list_builders(running=True)]
        if builder_name:
            builders = {builder_name: builders[builder_name], } if builder_name in builders else {}
        if builders:
            SSHAgent.start(verbose=verbose)
            SSHAgent.add_keys(set(b["key_file"] for b in builders), verbose)
            ParallelObject(builders, timeout=20).run(get_builder_docker_client, ignore_exceptions=True)
            log.info("%d builders from %d available to scan for Docker resources", len(docker_clients), len(builders))
        else:
            log.warning("No builders found")

    return docker_clients


def list_resources_docker(tags_dict: Optional[dict] = None,
                          builder_name: Optional[str] = None,
                          running: bool = False,
                          group_as_builder: bool = False,
                          verbose: bool = False) -> Dict[str, Union[list, dict]]:
    log = LOGGER if verbose else Mock()
    filters = {}

    current_container_id = docker_current_container_id()

    if tags_dict:
        filters["label"] = []
        for key, value in tags_dict.items():
            filters["label"].append(key if key == "NodeType" else f"{key}={value}")

    containers = {}
    images = {}

    def get_containers(builder_name: str, docker_client: docker.DockerClient) -> None:
        log.info("%s: scan for Docker containers", builder_name)
        containers_list = docker_client.containers.list(filters=filters, sparse=True)
        if node_type_values := tags_dict.get("NodeType"):
            filtered_containers_list = []
            for container in containers_list:
                container.reload()
                if container.labels.get("NodeType") in node_type_values:
                    filtered_containers_list.append(container)
            containers_list = filtered_containers_list
        if current_container_id:
            containers_list = [container for container in containers_list if container.id != current_container_id]
        if running:
            containers_list = [container for container in containers_list if container.status == "running"]
        else:
            containers_list = [container for container in containers_list if container.status != "removing"]
        if containers_list:
            log.info("%s: found %d containers", builder_name, len(containers_list))
            containers[builder_name] = containers_list

    def get_images(builder_name: str, docker_client: docker.DockerClient) -> None:
        log.info("%s: scan for Docker images", builder_name)
        images_list = docker_client.images.list(filters=filters)
        if images_list:
            log.info("%s: found %s images", builder_name, len(images_list))
            images[builder_name] = images_list

    docker_clients = tuple(list_clients_docker(builder_name=builder_name, verbose=verbose).items())

    ParallelObject(docker_clients, timeout=30).run(get_containers, ignore_exceptions=True, unpack_objects=True)
    ParallelObject(docker_clients, timeout=30).run(get_images, ignore_exceptions=True, unpack_objects=True)

    if not group_as_builder:
        containers = list(itertools.chain.from_iterable(containers.values()))
        images = list(itertools.chain.from_iterable(images.values()))

    return dict(containers=containers, images=images)


def aws_tags_to_dict(tags_list):
    tags_dict = {}
    if tags_list:
        for item in tags_list:
            tags_dict[item["Key"]] = item["Value"]
    return tags_dict

# pylint: disable=too-many-arguments


def list_instances_aws(tags_dict=None, region_name=None, running=False, group_as_region=False, verbose=False, availability_zone=None):
    """
    list all instances with specific tags AWS

    :param tags_dict: key-value pairs used for filtering
    :param region_name: name of the region to list
    :param running: get all running instances
    :param group_as_region: if True the results would be grouped into regions
    :param verbose: if True will log progress information
    :param availability_zone: availability zone letter (e.g. 'a', 'b', 'c')

    :return: instances dict where region is a key
    """
    instances = {}
    aws_regions = [region_name] if region_name else all_aws_regions()

    def get_instances(region):
        if verbose:
            LOGGER.info('Going to list aws region "%s"', region)
        time.sleep(random.random())
        client: EC2Client = boto3.client('ec2', region_name=region)
        custom_filter = []
        if tags_dict:
            custom_filter = [{'Name': 'tag:{}'.format(key),
                              'Values': value if isinstance(value, list) else [value]}
                             for key, value in tags_dict.items()]
        response = client.describe_instances(Filters=custom_filter)
        instances[region] = [instance for reservation in response['Reservations'] for instance in reservation[
            'Instances']]

        if verbose:
            LOGGER.info("%s: done [%s/%s]", region, len(list(instances.keys())), len(aws_regions))

    ParallelObject(aws_regions, timeout=100, num_workers=len(aws_regions)).run(get_instances, ignore_exceptions=True)

    for curr_region_name in instances:
        if running:
            instances[curr_region_name] = [i for i in instances[curr_region_name] if i['State']['Name'] == 'running']
        else:
            instances[curr_region_name] = [i for i in instances[curr_region_name]
                                           if not i['State']['Name'] == 'terminated']
    if availability_zone is not None:
        # filter by availability zone (a, b, c, etc.)
        for curr_region_name in instances:
            instances[curr_region_name] = [i for i in instances[curr_region_name]
                                           if i['Placement']['AvailabilityZone'] == curr_region_name + availability_zone]
    if not group_as_region:
        instances = list(itertools.chain(*list(instances.values())))  # flatten the list of lists
        total_items = len(instances)
    else:
        total_items = sum([len(value) for _, value in instances.items()])

    if verbose:
        LOGGER.info("Found total of {} instances.".format(total_items))

    return instances


def list_elastic_ips_aws(tags_dict=None, region_name=None, group_as_region=False, verbose=False):
    """
    list all elastic ips with specific tags AWS

    :param tags_dict: key-value pairs used for filtering
    :param region_name: name of the region to list
    :param group_as_region: if True the results would be grouped into regions
    :param verbose: if True will log progress information

    :return: instances dict where region is a key
    """
    elastic_ips = {}
    aws_regions = [region_name] if region_name else all_aws_regions()

    def get_elastic_ips(region):
        if verbose:
            LOGGER.info('Going to list aws region "%s"', region)
        time.sleep(random.random())
        client: EC2Client = boto3.client('ec2', region_name=region)
        custom_filter = []
        if tags_dict:
            custom_filter = [{'Name': 'tag:{}'.format(key),
                              'Values': value if isinstance(value, list) else [value]}
                             for key, value in tags_dict.items()]
        response = client.describe_addresses(Filters=custom_filter)
        elastic_ips[region] = response['Addresses']
        if verbose:
            LOGGER.info("%s: done [%s/%s]", region, len(list(elastic_ips.keys())), len(aws_regions))

    ParallelObject(aws_regions, timeout=100, num_workers=len(aws_regions)).run(
        get_elastic_ips, ignore_exceptions=True)

    if not group_as_region:
        elastic_ips = list(itertools.chain(*list(elastic_ips.values())))  # flatten the list of lists
        total_items = elastic_ips
    else:
        total_items = sum([len(value) for _, value in elastic_ips.items()])
    if verbose:
        LOGGER.info("Found total of %s ips.", total_items)
    return elastic_ips


def list_test_security_groups(tags_dict=None, region_name=None, group_as_region=False, verbose=False):
    """
    list all security groups with specific tags AWS

    :param tags_dict: key-value pairs used for filtering
    :param region_name: name of the region to list
    :param group_as_region: if True the results would be grouped into regions
    :param verbose: if True will log progress information

    :return: security groups dict where region is a key
    """
    security_groups = {}
    aws_regions = [region_name] if region_name else all_aws_regions()

    def get_security_groups_ips(region):
        if verbose:
            LOGGER.info('Going to list aws region "%s"', region)
        time.sleep(random.random())
        client: EC2Client = boto3.client('ec2', region_name=region)
        custom_filter = [{'Name': 'tag:{}'.format(key),
                          'Values': value if isinstance(value, list) else [value]}
                         for key, value in tags_dict.items() if key != 'NodeType']
        response = client.describe_security_groups(Filters=custom_filter)
        security_groups[region] = response['SecurityGroups']
        if verbose:
            LOGGER.info("%s: done [%s/%s]", region, len(list(security_groups.keys())), len(aws_regions))

    ParallelObject(aws_regions, timeout=100,  num_workers=len(aws_regions)
                   ).run(get_security_groups_ips, ignore_exceptions=True)

    if not group_as_region:
        security_groups = list(itertools.chain(*list(security_groups.values())))  # flatten the list of lists
        total_items = security_groups
    else:
        total_items = sum([len(value) for _, value in security_groups.items()])
    if verbose:
        LOGGER.info("Found total of %s ips.", total_items)
    return security_groups


def list_load_balancers_aws(tags_dict=None, regions=None, group_as_region=False, verbose=False):
    """
    list all load balancers with specific tags AWS

    :param tags_dict: key-value pairs used for filtering
    :param regions: list of the AWS regions to consider
    :param group_as_region: if True the results would be grouped into regions
    :param verbose: if True will log progress information

    :return: load balancers dict where region is a key
    """
    load_balancers = {}
    aws_regions = regions or all_aws_regions()

    def get_load_balancers(region):
        if verbose:
            LOGGER.info('Going to list aws region "%s"', region)
        time.sleep(random.random())
        tagging = boto3.client('resourcegroupstaggingapi', region_name=region)
        paginator = tagging.get_paginator('get_resources')
        tag_filter = [{'Key': key, 'Values': [value]}
                      for key, value in tags_dict.items() if key != 'NodeType']

        tag_mappings = itertools.chain.from_iterable(
            page['ResourceTagMappingList']
            for page in paginator.paginate(TagFilters=tag_filter, ResourceTypeFilters=['elasticloadbalancing:loadbalancer'])
        )
        load_balancers[region] = list(tag_mappings)
        if verbose:
            LOGGER.info("%s: done [%s/%s]", region, len(list(load_balancers.keys())), len(aws_regions))

    ParallelObject(aws_regions, timeout=100, num_workers=len(aws_regions)
                   ).run(get_load_balancers, ignore_exceptions=False)

    if not group_as_region:
        load_balancers = list(itertools.chain(*list(load_balancers.values())))  # flatten the list of lists
        total_items = load_balancers
    else:
        total_items = sum([len(value) for _, value in load_balancers.items()])
    if verbose:
        LOGGER.info("Found total of %s ips.", total_items)
    return load_balancers


def list_cloudformation_stacks_aws(tags_dict=None, regions=None, group_as_region=False, verbose=False):
    """
    list all cloudformation stacks with specific tags AWS

    :param tags_dict: key-value pairs used for filtering
    :param regions: list of the AWS regions to consider
    :param group_as_region: if True the results would be grouped into regions
    :param verbose: if True will log progress information

    :return: cloudformation staacks dict where region is a key
    """
    cloudformation_stacks = {}
    aws_regions = regions or all_aws_regions()

    def get_stacks(region):
        if verbose:
            LOGGER.info('Going to list aws region "%s"', region)
        time.sleep(random.random())
        tagging = boto3.client('resourcegroupstaggingapi', region_name=region)
        paginator = tagging.get_paginator('get_resources')
        tag_filter = [{'Key': key, 'Values': [value]}
                      for key, value in tags_dict.items() if key != 'NodeType']

        tag_mappings = itertools.chain.from_iterable(
            page['ResourceTagMappingList']
            for page in paginator.paginate(TagFilters=tag_filter, ResourceTypeFilters=['cloudformation:stack'])
        )
        cloudformation_stacks[region] = list(tag_mappings)
        if verbose:
            LOGGER.info("%s: done [%s/%s]", region, len(list(cloudformation_stacks.keys())), len(aws_regions))

    ParallelObject(aws_regions, timeout=100, num_workers=len(aws_regions)).run(get_stacks, ignore_exceptions=False)

    if not group_as_region:
        cloudformation_stacks = list(itertools.chain(*list(cloudformation_stacks.values()))
                                     )  # flatten the list of lists
        total_items = cloudformation_stacks
    else:
        total_items = sum([len(value) for _, value in cloudformation_stacks.items()])
    if verbose:
        LOGGER.info("Found total of %s ips.", total_items)
    return cloudformation_stacks


def list_launch_templates_aws(tags_dict=None, regions=None, verbose=False):
    """
    List all launch templates with specific tags.

    :param tags_dict: key-value pairs used for resource filtering
    :param regions: list of the AWS regions to consider
    :param verbose: if True will log progress information

    :return: launch templates dict where region is a key
    """
    launch_templates, aws_regions = {}, regions or all_aws_regions()

    def get_launch_templates(region):
        if verbose:
            LOGGER.info("Going to list AWS region '%s'", region)
        time.sleep(random.random())
        ec2_client = boto3.client("ec2", region_name=region)
        paginator = ec2_client.get_paginator("describe_launch_templates")
        tags_filters = [{"Name": f"tag:{k}", "Values": [v]} for k, v in tags_dict.items() if k != "NodeType"]
        launch_templates[region] = []
        for page in paginator.paginate(Filters=tags_filters):
            if current_launch_templates := page["LaunchTemplates"]:
                launch_templates[region].extend(current_launch_templates)
        if verbose:
            LOGGER.info("%s: done [%s/%s]", region, len(launch_templates), len(aws_regions))

    ParallelObject(aws_regions, timeout=120, num_workers=len(aws_regions)).run(
        get_launch_templates, ignore_exceptions=False)

    if verbose:
        total_items = sum([len(value) for value in launch_templates.values()])
        LOGGER.info("Found total of %s launch templates.", total_items)
    return launch_templates


def get_all_gce_regions():
    region_client, info = get_gce_compute_regions_client()
    all_gce_regions = [region_obj.name for region_obj in region_client.list(project=info['project_id'])]
    return all_gce_regions


@singledispatch
def gce_meta_to_dict(metadata):
    raise NotImplementedError(f"{type(metadata)=} isn't supported")


@gce_meta_to_dict.register
def _(metadata: GceMetadata):
    meta_dict = {}
    data = metadata.items
    if data:
        for item in data:
            key = item.key
            if key:  # sometimes key is empty string
                meta_dict[key] = item.value
    return meta_dict


@gce_meta_to_dict.register
def _(metadata: dict):
    meta_dict = {}
    data = metadata.get("items")
    if data:
        for item in data:
            key = item["key"]
            if key:  # sometimes key is empty string
                meta_dict[key] = item["value"]
    return meta_dict


def filter_gce_by_tags(tags_dict, instances: list[GceInstance]) -> list[GceInstance]:
    filtered_instances = []

    for instance in instances:
        if 'Name' in tags_dict.keys() and tags_dict['Name'] == instance.name:
            filtered_instances.append(instance)
            continue

        tags = gce_meta_to_dict(instance.metadata)
        for tag_k, tag_v in tags_dict.items():
            if tag_k not in tags or (tags[tag_k] not in tag_v if isinstance(tag_v, list) else tags[tag_k] != tag_v):
                break
        else:
            filtered_instances.append(instance)

    return filtered_instances


def list_instances_gce(tags_dict: Optional[dict] = None,
                       running: bool = False,
                       verbose: bool = True) -> list[GceInstance]:
    """List all instances with specific tags GCE."""
    instances_client, info = get_gce_compute_instances_client()
    if verbose:
        LOGGER.info("Going to get all instances from GCE")
    all_gce_instances = instances_client.aggregated_list(project=info['project_id'])
    # filter instances by tags since google doesn't offer any filtering
    all_instances = []
    for _, response in all_gce_instances:
        if response.instances:
            all_instances.extend(response.instances)
    if tags_dict:
        instances = filter_gce_by_tags(tags_dict=tags_dict, instances=all_instances)
    else:
        instances = all_instances

    if running:
        instances = [i for i in instances if i.status == 'RUNNING']
    else:
        instances = [i for i in instances if not i.status == 'TERMINATED']
    if verbose:
        LOGGER.info("Done. Found total of %s instances.", len(instances))
    return instances


def list_static_ips_gce(verbose=False):
    addresses_client, info = get_gce_compute_addresses_client()
    if verbose:
        LOGGER.info("Getting all GCE static IPs...")
    all_static_ips = []
    for _, response in list(addresses_client.aggregated_list(project=info['project_id'])):
        if response.addresses:
            all_static_ips.extend(response.addresses)

    if verbose:
        LOGGER.info("Found total %s GCE static IPs.", len(all_static_ips))
    return all_static_ips


def list_clusters_gke(tags_dict: Optional[dict] = None, verbose: bool = False) -> list:
    clusters = GkeCleaner().list_gke_clusters()
    if tags_dict:
        clusters = filter_k8s_clusters_by_tags(tags_dict, clusters)
    if verbose:
        LOGGER.info("Done. Found total of %s GKE clusters.", len(clusters))
    return clusters


def list_clusters_eks(tags_dict: Optional[dict] = None, regions: list = None,
                      verbose: bool = False) -> List[EksClusterForCleaner]:
    class EksCleaner:
        name = f"eks-cleaner-{uuid.uuid4()!s:.8}"
        _containers = {}
        tags = {}

        @cached_property
        def eks_client(self):  # pylint: disable=no-self-use
            return

        def list_clusters(self) -> list:  # pylint: disable=no-self-use
            eks_clusters = []
            for aws_region in regions or all_aws_regions():
                try:
                    cluster_names = boto3.client('eks', region_name=aws_region).list_clusters()['clusters']
                except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                    LOGGER.error("Failed to get list of EKS clusters in the '%s' region: %s", aws_region, exc)
                    return []
                for cluster_name in cluster_names:
                    try:
                        eks_clusters.append(EksClusterForCleaner(cluster_name, aws_region))
                    except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                        LOGGER.error("Failed to get body of cluster on EKS: %s", exc)
            return eks_clusters

    clusters = EksCleaner().list_clusters()

    if tags_dict:
        clusters = filter_k8s_clusters_by_tags(tags_dict, clusters)

    if verbose:
        LOGGER.info("Done. Found total of %s EKS clusters.", len(clusters))

    return clusters


def filter_k8s_clusters_by_tags(tags_dict: dict, clusters: list[
        Union["EksClusterForCleaner", "GkeClusterForCleaner"]]) -> list[  # noqa: F821
            Union["EksClusterForCleaner", "GkeClusterForCleaner"]]:  # noqa: F821
    if "NodeType" in tags_dict and "k8s" not in tags_dict.get("NodeType"):
        return []

    return filter_gce_by_tags(tags_dict={k: v for k, v in tags_dict.items() if k != 'NodeType'},
                              instances=clusters)


@lru_cache
def get_scylla_ami_versions(region_name: str, arch: AwsArchType = 'x86_64', version: str = None) -> list[EC2Image]:
    """Get the list of all the formal scylla ami from specific region."""
    name_filter = "ScyllaDB *"

    if version and version != "all":
        name_filter = f"ScyllaDB *{version.replace('enterprise-', 'Enterprise ')}"

        if len(version.split('.')) < 3:
            # if version is not exact version, we need to add the wildcard to the end, to catch all minor versions
            name_filter = f"{name_filter}*"

    ec2_resource: EC2ServiceResource = boto3.resource('ec2', region_name=region_name)
    images = []
    for client, owner in zip((ec2_resource, get_scylla_images_ec2_resource(region_name=region_name)),
                             SCYLLA_AMI_OWNER_ID_LIST):
        images += client.images.filter(
            Owners=[owner],
            Filters=[
                {'Name': 'name', 'Values': [name_filter]},
                {'Name': 'architecture', 'Values': [arch]},
            ],
        )
    images = sorted(images, key=lambda x: x.creation_date, reverse=True)
    images = [image for image in images if image.tags and 'debug' not in {
        i['Key']: i['Value'] for i in image.tags}.get('Name', '')]

    return images


@lru_cache
def get_scylla_gce_images_versions(project: str = SCYLLA_GCE_IMAGES_PROJECT, version: str = None) -> list[GceImage]:
    # Server-side resource filtering described in Google SDK reference docs:
    #   API reference: https://cloud.google.com/compute/docs/reference/rest/v1/images/list
    #   RE2 syntax: https://github.com/google/re2/blob/master/doc/syntax.txt
    # or you can see brief explanation here:
    #   https://github.com/apache/libcloud/blob/trunk/libcloud/compute/drivers/gce.py#L274
    filters = "(family eq 'scylla(-enterprise)?')(name ne .+-build-.+)"

    if version and version != "all":
        filters += f"(name eq 'scylla(db)?(-enterprise)?-{version.replace('.', '-')}"
        if 'rc' not in version and len(version.split('.')) < 3:
            filters += "(-\\d)?(\\d)?(\\d)?(-rc)?(\\d)?(\\d)?')"
        else:
            filters += "')"
    images_client, _ = get_gce_compute_images_client()
    return sorted(
        images_client.list(ListImagesRequest(filter=filters, project=project)),
        key=lambda x: x.creation_timestamp,
        reverse=True,
    )


ScyllaProduct = Literal['scylla', 'scylla-enterprise']


def get_latest_scylla_ami_release(region: str = 'eu-west-1',
                                  product: ScyllaProduct = 'scylla') -> str:
    """
        Get the latest release, base on the formal AMIs published
    """

    if product == 'scylla-enterprise':
        filter_regex = re.compile(r"(rc|dev)", flags=re.IGNORECASE)
        version_regex = re.compile(r"enterprise\s*(\d*\.\d*\.\d*)", flags=re.IGNORECASE)
    else:
        filter_regex = re.compile(r"(enterprise|rc|dev)", flags=re.IGNORECASE)
        version_regex = re.compile(r"(\d*\.\d*\.\d*)", flags=re.IGNORECASE)
    versions = []
    for ami in get_scylla_ami_versions(region_name=region):
        if not filter_regex.search(ami.name):
            if version := version_regex.search(ami.name):
                versions.append(Version(version.group(1)))
    return str(max(versions))


def get_latest_scylla_release(product: Literal['scylla', 'scylla-enterprise']) -> str:
    """
    get latest advertised scylla version from the same service scylla_setup is getting it
    """

    product = product.lstrip('scylla-')
    url = 'https://repositories.scylladb.com/scylla/check_version?system={}'
    version = requests.get(url.format(product)).json()
    return version['version']


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
    except Exception:  # pylint: disable=broad-except  # noqa: BLE001
        return False


class FileFollowerIterator():  # pylint: disable=too-few-public-methods
    def __init__(self, filename, thread_obj):
        self.filename = filename
        self.thread_obj = thread_obj

    def __iter__(self):
        with open(self.filename, encoding="utf-8") as input_file:
            line = ''
            poller = select.poll()  # pylint: disable=no-member
            registered = False
            while not self.thread_obj.stopped():
                if not registered:
                    poller.register(input_file, select.POLLIN)  # pylint: disable=no-member
                    registered = True
                if poller.poll(100):
                    line += input_file.readline()
                if not line or not line.endswith('\n'):
                    time.sleep(0.1)
                    continue
                poller.unregister(input_file)
                registered = False
                yield line
                line = ''
            yield line


class FileFollowerThread():
    def __init__(self):
        self.executor = concurrent.futures.ThreadPoolExecutor(1)  # pylint: disable=consider-using-with
        self._stop_event = threading.Event()
        self.future = None

    def __enter__(self):
        self.start()
        return self

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


class ScyllaCQLSession:
    def __init__(self, session, cluster, verbose=True):
        self.session = session
        self.cluster = cluster
        self.verbose = verbose

    def __enter__(self):
        execute_orig = self.session.execute
        execute_async_orig = self.session.execute_async

        def execute_verbose(*args, **kwargs):
            if args:
                query = args[0]
            else:
                query = kwargs.get("query")
            LOGGER.debug("Executing CQL '%s' ...", query)
            return execute_orig(*args, **kwargs)

        def execute_async_verbose(*args, **kwargs):
            if args:
                query = args[0]
            else:
                query = kwargs.get("query")
            LOGGER.debug("Executing CQL '%s' ...", query)
            return execute_async_orig(*args, **kwargs)

        if self.verbose:
            self.session.execute = execute_verbose
            self.session.execute_async = execute_async_verbose
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cluster.shutdown()


def get_free_port(address: str = '', ports_to_try: Iterable[int] = (0,)) -> int:
    for port in ports_to_try:
        try:
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                sock.bind((address, port))
                return sock.getsockname()[1]
        except OSError:
            pass
    raise RuntimeError("Can't allocate a free port")


@retrying(n=60, sleep_time=5, allowed_exceptions=(OSError,))
def wait_for_port(host, port):
    socket.create_connection((host, port)).close()


def can_connect_to(ip: str, port: int, timeout: int = 1) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.settimeout(timeout)
    result = sock.connect_ex((ip, port))
    sock.close()
    return result == 0


def get_branched_ami(scylla_version: str, region_name: str, arch: AwsArchType = None) -> list[EC2Image]:
    """
    Get a list of AMIs, based on version match

    :param scylla_version: branch version to look for, ex. 'branch-2019.1:latest', 'branch-3.1:all'
    :param region_name: the region to look AMIs in
    :param arch: image architecture, it is either x86_64 or arm64
    :return: list of ec2.images
    """
    branch, build_id = scylla_version.split(":", 1)
    filters = [
        {"Name": "tag:branch", "Values": [branch, ], },
        {"Name": "architecture", "Values": [arch, ], },
        {"Name": "tag:build_mode", "Values": ["release", ], },
    ]

    LOGGER.info("Looking for AMIs match [%s]", scylla_version)
    ec2_resource: EC2ServiceResource = boto3.resource("ec2", region_name=region_name)
    images = []

    for client, owner in zip((ec2_resource, get_scylla_images_ec2_resource(region_name=region_name)),
                             SCYLLA_AMI_OWNER_ID_LIST):
        if build_id not in ("latest", "all",):
            images += [
                client.images.filter(Owners=[owner],
                                     Filters=filters + [{'Name': 'tag:build-id', 'Values': [build_id, ], }]),
                client.images.filter(Owners=[owner],
                                     Filters=filters + [{'Name': 'tag:build_id', 'Values': [build_id, ], }]),
            ]
        else:
            images += [client.images.filter(Owners=[owner], Filters=filters), ]

    images = sorted(itertools.chain.from_iterable(images), key=lambda x: x.creation_date, reverse=True)
    images = [image for image in images if not (image.name.startswith('debug-') or '-debug-' in image.name)]

    assert images, f"AMIs for {scylla_version=} with {arch} architecture not found in {region_name}"

    if build_id == "all":
        return images
    return images[:1]


def get_ami_images(branch: str, region: str, arch: AwsArchType) -> list:
    """
    Retrieve the AMI images data.
    The data points retrieved are: ["Backend", "Name", "ImageId", "CreationDate", "BuildId", "Arch", "ScyllaVersion"]
    """
    rows = []

    try:
        amis = get_branched_ami(scylla_version=branch, region_name=region, arch=arch)
    except AssertionError:
        return rows

    for ami in amis:
        tags = {i['Key']: i['Value'] for i in ami.tags}
        rows.append(["AWS", ami.name, ami.image_id, ami.creation_date, tags.get("Name"), tags.get(
            'build-id', tags.get("build_id", r"N\A"))[:6], tags.get('arch'), tags.get('ScyllaVersion'), ami.owner_id])

    return rows


def get_ec2_image_name_tag(ami: EC2Image) -> str:
    if ami.tags:
        for tag in ami.tags:
            if tag["Key"] == "Name":
                return tag["Value"]
    return ""


@lru_cache
def convert_name_to_ami_if_needed(ami_id_param: str, region_names: list[str],) -> str:
    """
    convert image name in ami_id param to ami_ids
    Firstly trying to find name in 'tag:Name' - for ScyllaDB images case
    then in 'name' - for ScyllaDB images case for public images like Ubuntu

    usage example:
    convert_name_to_ami_if_needed('scylla-5.1.4-x86_64-2023-01-22T17-58-39Z' ,
                                  ['eu-west-1', 'eu-west-2', 'us-west-2', 'us-east-1', 'eu-north-1', 'eu-central-1'])
    returns: 'ami-042cf1bc21e30ce60 ami-0370fbb289d8310d2 ami-09808043b7fcdb244
              ami-00c1a16b5136fbb7c ami-0226185e7d7951b6a ami-0bc7811c417b0eb09'

     convert_name_to_ami_if_needed('ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-20221206' ,
                                   'eu-west-1', 'eu-west-2', 'us-west-2', 'us-east-1', 'eu-north-1', 'eu-central-1'])
    returns: 'ami-026e72e4e468afa7b ami-01b8d743224353ffe ami-03f8756d29f0b5f21
              ami-06878d265978313ca ami-03df6dea56f8aa618 ami-0039da1f3917fa8e3'
    """
    param_values = ami_id_param.split()

    if len(param_values) == 1 and param_values[0].startswith("resolve:ssm:"):
        ssm_name = param_values[0].replace("resolve:ssm:", "")
        ami_list: list[str] = []
        for region_name in region_names:
            ami_list.append(get_ssm_ami(ssm_name, region_name=region_name))
        return " ".join(ami_list)

    if len(param_values) == 1 and param_values[0].startswith("resolve:owner:"):
        owner_details = param_values[0].replace("resolve:owner:", "")
        ami_list: list[str] = []
        for region_name in region_names:
            ami_list.append(get_by_owner_ami(owner_details, region_name=region_name))
        return " ".join(ami_list)

    if len(param_values) == 1 and not param_values[0].startswith("ami-"):
        ami_mapping: dict[str, str] = OrderedDict()
        for region_name in region_names:
            ec2_resource = boto3.resource('ec2', region_name=region_name)
            for client, _ in zip((ec2_resource, get_scylla_images_ec2_resource(region_name=region_name)),
                                 SCYLLA_AMI_OWNER_ID_LIST):
                for tag_name in ("tag:Name", "name"):
                    if images := sorted(
                            client.images.filter(Filters=[{'Name': tag_name, 'Values': [param_values[0]]}]),
                            key=lambda x: x.creation_date, reverse=True):
                        ami_mapping[region_name] = images[0].image_id
                        break
                else:
                    continue
        if not len(ami_mapping) == len(region_names):
            raise ValueError(
                f"Can't convert name '{ami_id_param}' to AMI_id, no image found in regions {region_names}")
        return " ".join(ami_mapping.values())
    return ami_id_param


def get_ami_images_versioned(region_name: str, arch: AwsArchType, version: str) -> list[list[str]]:
    return [["AWS", ami.name, ami.image_id, ami.creation_date, get_ec2_image_name_tag(ami)]
            for ami in get_scylla_ami_versions(region_name=region_name, arch=arch, version=version)]


def get_gce_images_versioned(version: str = None) -> list[list[str]]:
    return [["GCE", image.name, image.self_link, image.creation_timestamp]
            for image in get_scylla_gce_images_versions(version=version)]


def get_gce_images(branch: str, arch: AwsArchType) -> list:
    """
    Retrieve the GCE images data.
    The data points retrieved are: ["Backend", "Name", "ImageId", "CreationDate", "BuildId", "Arch", "ScyllaVersion"]
    """
    rows = []

    try:
        gce_images = get_branched_gce_images(scylla_version=branch, arch=arch)
    except AssertionError:
        return rows

    for image in gce_images:
        rows.append([
            "GCE",
            image.name,
            image.self_link,
            image.creation_timestamp,
            image.labels.get("build-id") or image.name.rsplit("-build-", maxsplit=1)[-1],
            image.labels.get("arch"),
            image.labels.get("scylla_version")
        ])

    return rows


def create_pretty_table(rows: list[str] | list[list[str]], field_names: list[str]) -> PrettyTable:
    tbl = PrettyTable(field_names=field_names, align="l")

    for row in rows:
        tbl.add_row(row)

    return tbl


def get_branched_gce_images(
        scylla_version: str,
        project: str = SCYLLA_GCE_IMAGES_PROJECT,
        arch: AwsArchType = None) -> list[GceImage]:
    branch, build_id = scylla_version.split(":", 1)

    # Server-side resource filtering described in Google SDK reference docs:
    #   API reference: https://cloud.google.com/compute/docs/reference/rest/v1/images/list
    #   RE2 syntax: https://github.com/google/re2/blob/master/doc/syntax.txt
    # or you can see brief explanation here:
    #   https://github.com/apache/libcloud/blob/trunk/libcloud/compute/drivers/gce.py#L274
    filters = f"(family eq scylla)(labels.branch eq {branch})(name ne debug-.*)"

    if build_id not in ("latest", "all",):
        # filters += f"(labels.build-id eq {build_id})"  # asked releng to add `build-id' label too, but
        filters += f"(name eq .+-build-{build_id})"  # use BUILD_ID from an image name for now

    if arch:
        filters += f"(labels.arch eq {arch.replace('_', '-')})"

    LOGGER.info("Looking for GCE images match [%s]", scylla_version)
    images_client, _ = get_gce_compute_images_client()
    images = sorted(
        images_client.list(ListImagesRequest(filter=filters, project=project)),
        key=lambda x: x.creation_timestamp,
        reverse=True,
    )

    assert images, f"GCE images for {scylla_version=} not found"

    if build_id == "all":
        return images
    return images[:1]


@lru_cache()
def ami_built_by_scylla(ami_id: str, region_name: str) -> bool:
    all_tags = get_ami_tags(ami_id, region_name)
    if owner_id := all_tags.get('owner_id'):
        return owner_id in SCYLLA_AMI_OWNER_ID_LIST
    else:
        return False


@lru_cache()
def get_ami_tags(ami_id, region_name):
    """
    Get a list of tags of a specific AMI

    :param ami_id:
    :param region_name: the region to look AMIs in
    :return: dict of tags
    """
    scylla_images_ec2_resource = get_scylla_images_ec2_resource(region_name=region_name)
    new_test_image = scylla_images_ec2_resource.Image(ami_id)
    new_test_image.reload()
    if new_test_image and new_test_image.meta.data and new_test_image.tags:
        res = {i['Key']: i['Value'] for i in new_test_image.tags}
        res['owner_id'] = new_test_image.owner_id
        return res
    else:
        ec2_resource: EC2ServiceResource = boto3.resource('ec2', region_name=region_name)
        test_image = ec2_resource.Image(ami_id)
        test_image.reload()
        if test_image and test_image.meta.data and test_image.tags:
            res = {i['Key']: i['Value'] for i in test_image.tags}
            res['owner_id'] = test_image.owner_id
            return res
        else:
            return {}


def get_db_tables(session, keyspace_name, node, with_compact_storage=True):
    """
    Return tables from keystore based on their compact storage feature
    Arguments:
        session -- DB session
        ks -- Keypsace name
        with_compact_storage -- If True, return non compact tables, if False, return compact tables

    """
    output = []
    for row in list(session.execute(f"select table_name from system_schema.tables where keyspace_name='{keyspace_name}'")):
        try:
            create_table_statement = node.run_cqlsh(f"describe {keyspace_name}.{row.table_name}").stdout.upper()
        except (UnexpectedExit, Libssh2_UnexpectedExit) as err:
            # SCT issue https://github.com/scylladb/scylla-cluster-tests/issues/7240
            # May happen when disrupt_add_remove_dc nemesis run in parallel to the disrupt_add_drop_column
            LOGGER.error("Failed to describe '%s.%s' table. Maybe the table has been deleted. Error: %s",
                         keyspace_name, row.table_name, err.result.stderr)
            continue

        if with_compact_storage is None or (("WITH COMPACT STORAGE" in create_table_statement) == with_compact_storage):
            output.append(row.table_name)

    return output


# Add @retrying to prevent situation when nemesis failed on connection timeout when try to receive the
# keyspace and table for the test


def remove_files(path):
    LOGGER.debug("Remove path %s", path)
    try:
        if os.path.isdir(path):
            shutil.rmtree(path=path, ignore_errors=True)
        if os.path.isfile(path):
            os.remove(path)
    except Exception as details:  # pylint: disable=broad-except  # noqa: BLE001
        LOGGER.error("Error during remove archived logs %s", details)
        LOGGER.info("Remove temporary data manually: \"%s\"", path)


def create_remote_storage_dir(node, path='') -> Optional[str, None]:
    node_remote_dir = '/tmp'
    if not path:
        path = node.name
    try:
        remote_dir = os.path.join(node_remote_dir, path)
        result = node.remoter.run(f'mkdir -p {remote_dir}', ignore_status=True)

        if result.exited > 0:
            LOGGER.error(
                'Remote storing folder not created.\n %s', result)
            remote_dir = node_remote_dir

    except Exception as details:  # pylint: disable=broad-except  # noqa: BLE001
        LOGGER.error("Error during creating remote directory %s", details)
        return None

    return remote_dir


def format_timestamp(timestamp):
    try:
        # try convert seconds
        return datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        # try convert miliseconds
        return datetime.datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')


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


# Make it mockable.
def _s3_download_file(client, bucket, key, local_file_path):
    return client.download_file(bucket, key, local_file_path)


def s3_download_dir(bucket, path, target):
    """
    Downloads recursively the given S3 path to the target directory.
    :param bucket: the name of the bucket to download from
    :param path: The S3 directory to download.
    :param target: the local directory to download the files to.
    """

    client: S3Client = boto3.client('s3', region_name=DEFAULT_AWS_REGION)

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
            os.makedirs(local_file_dir, exist_ok=True)
            LOGGER.info("Downloading %s from s3 to %s", key['Key'], local_file_path)
            _s3_download_file(client, bucket, key['Key'], local_file_path)


def gce_download_dir(bucket, path, target):
    """
    Downloads recursively the given google storage path to the target directory.
    :param bucket: the name of the bucket to download from
    :param path: The google storage directory to download.
    :param target: the local directory to download the files to.
    """

    storage_client, _ = get_gce_storage_client()

    if not path.endswith('/'):
        path += '/'
    if path.startswith('/'):
        path = path[1:]
    blobs = storage_client.list_blobs(bucket_or_name=bucket, prefix=path)
    for obj in blobs:
        obj: GceBlob
        if obj.name in [".", "..", path]:
            continue
        rel_path = obj.name[len(path):]
        local_file_path = os.path.join(target, rel_path)

        local_file_dir = os.path.dirname(local_file_path)
        os.makedirs(local_file_dir, exist_ok=True)
        LOGGER.info("Downloading %s from gcp to %s", obj.name, local_file_path)
        obj.download_to_filename(filename=local_file_path)


def download_dir_from_cloud(url):
    """
    download a directory from AWS S3 or from google storage

    :param url: a url that starts with `s3://` or `gs://`
    :return: the temp directory create with the downloaded content
    """
    if not url:
        return url

    md5 = hashlib.md5()  # deepcode ignore insecureHash: can't change it
    md5.update(url.encode('utf-8'))
    tmp_dir = os.path.join('/tmp/download_from_cloud', md5.hexdigest())
    parsed = urlparse(url)
    LOGGER.info("Downloading [%s] to [%s]", url, tmp_dir)
    if os.path.isdir(tmp_dir) and os.listdir(tmp_dir):
        LOGGER.warning("[{}] already exists, skipping download".format(tmp_dir))
    elif url.startswith('s3://'):
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
        "monitor_nodes": [],
        "kubernetes_nodes": [],
    }

    for instance in instances:
        name = ""
        for tag in instance['Tags']:
            if tag['Key'] == 'Name':
                name = tag['Value']
                break
        if 'db-node' in name:
            filtered_instances["db_nodes"].append(instance)
        elif 'monitor-node' in name:
            filtered_instances["monitor_nodes"].append(instance)
        elif 'loader-node' in name:
            filtered_instances["loader_nodes"].append(instance)
        elif '-k8s-' in name:
            filtered_instances["kubernetes_nodes"].append(instance)

    return filtered_instances


def filter_gce_instances_by_type(instances):
    filtered_instances = {
        "db_nodes": [],
        "loader_nodes": [],
        "monitor_nodes": [],
        "kubernetes_nodes": [],
    }

    for instance in instances:
        if 'db-node' in instance.name:
            filtered_instances["db_nodes"].append(instance)
        elif 'monitor-node' in instance.name:
            filtered_instances["monitor_nodes"].append(instance)
        elif 'loader-node' in instance.name:
            filtered_instances["loader_nodes"].append(instance)
        elif '-k8s-' in instance.name:
            filtered_instances["kubernetes_nodes"].append(instance)

    return filtered_instances


def filter_docker_containers_by_type(containers):
    filtered_containers = {
        "db_nodes": [],
        "loader_nodes": [],
        "monitor_nodes": [],
        "kubernetes_nodes": [],
    }

    for container in containers:
        if "db-node" in container.name:
            filtered_containers["db_nodes"].append(container)
        elif "monitor-node" in container.name:
            filtered_containers["monitor_nodes"].append(container)
        elif "loader-node" in container.name:
            filtered_containers["loader_nodes"].append(container)
        elif '-k8s-' in container.name:
            filtered_containers["kubernetes_nodes"].append(container)
    return filtered_containers


SSH_KEY_DIR = "~/.ssh"
SSH_KEY_AWS_DEFAULT = "scylla_test_id_ed25519"
SSH_KEY_GCE_DEFAULT = "scylla_test_id_ed25519"


def get_aws_builders(tags=None, running=True):
    builders = []
    ssh_key_path = os.path.join(SSH_KEY_DIR, SSH_KEY_AWS_DEFAULT)

    aws_builders = list_instances_aws(tags_dict=tags, running=running)

    for aws_builder in aws_builders:
        builder_name = [tag["Value"] for tag in aws_builder["Tags"] if tag["Key"] == "Name"][0]
        builders.append({"builder": {
            "public_ip": aws_builder.get("PublicIpAddress"),
            "name": builder_name,
            "user": "jenkins",
            "key_file": os.path.expanduser(ssh_key_path)
        }})

    return builders


def get_gce_builders(tags=None, running=True):
    builders = []
    ssh_key_path = os.path.join(SSH_KEY_DIR, SSH_KEY_GCE_DEFAULT)

    gce_builders = list_instances_gce(tags_dict=tags, running=running)

    for gce_builder in gce_builders:
        builders.append({"builder": {
            "public_ip": gce_public_addresses(gce_builder)[0],
            "name": gce_builder.name,
            "user": "scylla-test",
            "key_file": os.path.expanduser(ssh_key_path)
        }})

    return builders


def list_builders(running=True):
    builder_tag = {"NodeType": "Builder"}
    aws_builders = get_aws_builders(builder_tag, running=running)
    gce_builders = get_gce_builders(builder_tag, running=running)

    return aws_builders + gce_builders


def get_builder_by_test_id(test_id):
    base_path_on_builder = "/home/jenkins/slave/workspace"
    found_builders = []

    builders = list_builders(running=True)

    def search_test_id_on_builder(builder):
        remoter = RemoteCmdRunnerBase.create_remoter(
            builder['public_ip'], user=builder["user"], key_file=builder["key_file"])

        LOGGER.info('Search on %s', builder['name'])
        result = remoter.run("find {where} -name test_id | xargs grep -rl {test_id}".format(where=base_path_on_builder,
                                                                                            test_id=test_id),
                             ignore_status=True, verbose=False)

        if not result.exited and result.stdout:
            builder["remoter"] = remoter
            path = result.stdout.strip()
            LOGGER.info("Builder name %s, ip %s, folder %s", builder['name'], builder['public_ip'], path)
            return {
                "builder": builder,
                "path": os.path.dirname(path)
            }
        else:
            LOGGER.info("Nothing found")
            return None

    if builders:
        search_obj = ParallelObject(builders, timeout=30, num_workers=len(builders))
        results = search_obj.run(search_test_id_on_builder, ignore_exceptions=True, unpack_objects=True)
        found_builders = [builder.result for builder in results if not builder.exc and builder.result]

    if not found_builders:
        LOGGER.info("Nothing found for %s", test_id)

    return found_builders


def get_post_behavior_actions(config):
    action_per_type = {
        "db_nodes": {"node_types": ["scylla-db"], "action": None},
        "monitor_nodes": {"node_types": ["monitor"], "action": None},
        "loader_nodes": {"node_types": ["loader"], "action": None},
        "k8s_cluster": {"node_types": ["k8s"], "action": None},
    }

    for key, value in action_per_type.items():
        value["action"] = config.get(f"post_behavior_{key}")

    match config.get("db_type"):
        case "cassandra":
            action_per_type["db_nodes"]["node_types"] = ["cs-db"]
        case "mixed":
            action_per_type["db_nodes"]["node_types"].append("cs-db")
        case "mixed_scylla":
            action_per_type["db_nodes"]["node_types"].append("oracle-db")

    return action_per_type


def search_test_id_in_latest(logdir):
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


def get_testrun_status(test_id=None, logdir=None, only_critical=False):
    testrun_dir = get_testrun_dir(logdir, test_id)
    if not testrun_dir:
        return None

    status = ""
    critical_log = os.path.join(testrun_dir, 'events_log/critical.log')
    error_log = os.path.join(testrun_dir, 'events_log/error.log')

    if os.path.exists(critical_log):
        with open(critical_log, encoding="utf-8") as file:
            status = file.readlines()

    if not only_critical and os.path.exists(error_log):
        with open(error_log, encoding="utf-8") as file:
            status += file.readlines()

    return status


def download_encrypt_keys():
    """
    Download certificate files of encryption at-rest from S3 KeyStore
    """
    ks = KeyStore()
    for pem_file in ['CA.pem', 'SCYLLADB.pem', 'hytrust-kmip-cacert.pem', 'hytrust-kmip-scylla.pem']:
        if not os.path.exists('./data_dir/encrypt_conf/%s' % pem_file):
            ks.download_file(pem_file, './data_dir/encrypt_conf/%s' % pem_file)


def normalize_ipv6_url(ip_address):
    """adds square brackets on the IPv6 address in the URL"""
    if ":" in ip_address:  # IPv6
        return "[%s]" % ip_address
    return ip_address


def rows_to_list(rows):
    return [list(row) for row in rows]


# Copied from dtest
class Page:  # pylint: disable=too-few-public-methods
    data = None

    def __init__(self):
        self.data = []

    def add_row(self, row):
        self.data.append(row)


# Copied from dtest
class PageFetcher:
    """
    Requests pages, handles their receipt,
    and provides paged data for testing.

    The first page is automatically retrieved, so an initial
    call to request_one is actually getting the *second* page!
    """
    pages = None
    error = None
    future = None
    requested_pages = None
    retrieved_pages = None
    retrieved_empty_pages = None

    def __init__(self, future):
        self.pages = []

        # the first page is automagically returned (eventually)
        # so we'll count this as a request, but the retrieved count
        # won't be incremented until it actually arrives
        self.requested_pages = 1
        self.retrieved_pages = 0
        self.retrieved_empty_pages = 0

        self.future = future
        self.future.add_callbacks(
            callback=self.handle_page,
            errback=self.handle_error
        )

        # wait for the first page to arrive, otherwise we may call
        # future.has_more_pages too early, since it should only be
        # called after the first page is returned
        self.wait(seconds=self.future.timeout)

    def handle_page(self, rows):
        # occasionally get a final blank page that is useless
        if rows == []:
            self.retrieved_empty_pages += 1
            return

        page = Page()
        self.pages.append(page)

        for row in rows:
            page.add_row(row)

        self.retrieved_pages += 1

    def handle_error(self, exc):
        self.error = exc
        LOGGER.error(self.error)
        raise exc

    def request_one(self, timeout=10):
        """
        Requests the next page if there is one.

        If the future is exhausted, this is a no-op.
        """
        if self.future.has_more_pages:
            self.future.start_fetching_next_page()
            self.requested_pages += 1
            self.wait(seconds=timeout)

        return self

    def request_all(self, timeout=10):
        """
        Requests any remaining pages.

        If the future is exhausted, this is a no-op.
        """
        while self.future.has_more_pages:
            self.future.start_fetching_next_page()
            self.requested_pages += 1
            self.wait(seconds=timeout)

        return self

    def wait(self, seconds=10):
        """
        Blocks until all *requested* pages have been returned.

        Requests are made by calling request_one and/or request_all.

        Raises RuntimeError if seconds is exceeded.
        """

        def error_message(msg):
            return "{}. Requested: {}; retrieved: {}; empty retrieved {}".format(
                msg, self.requested_pages, self.retrieved_pages, self.retrieved_empty_pages)

        def missing_pages():
            pages = self.requested_pages - (self.retrieved_pages + self.retrieved_empty_pages)
            assert pages >= 0, error_message('Retrieved too many pages')
            return pages

        missing = missing_pages()
        if missing <= 0:
            return self
        expiry = time.time() + seconds * missing

        while time.time() < expiry:
            if missing_pages() <= 0:
                return self
            # small wait so we don't need excess cpu to keep checking
            time.sleep(0.1)

        raise RuntimeError(error_message('Requested pages were not delivered before timeout'))

    def pagecount(self):
        """
        Returns count of *retrieved* pages which were not empty.

        Pages are retrieved by requesting them with request_one and/or request_all.
        """
        return len(self.pages)

    def num_results(self, page_num):
        """
        Returns the number of results found at page_num
        """
        return len(self.pages[page_num - 1].data)

    def num_results_all(self):
        return [len(page.data) for page in self.pages]

    def page_data(self, page_num):
        """
        Returns retreived data found at pagenum.

        The page should have already been requested with request_one and/or request_all.
        """
        return self.pages[page_num - 1].data

    def all_data(self):
        """
        Returns all retrieved data flattened into a single list (instead of separated into Page objects).

        The page(s) should have already been requested with request_one and/or request_all.
        """
        all_pages_combined = []
        for page in self.pages:
            all_pages_combined.extend(page.data[:])

        return all_pages_combined

    @property  # make property to match python driver api
    def has_more_pages(self):
        """
        Returns bool indicating if there are any pages not retrieved.
        """
        return self.future.has_more_pages


def reach_enospc_on_node(target_node):
    no_space_log_reader = target_node.follow_system_log(patterns=['No space left on device'])

    def approach_enospc():
        if bool(list(no_space_log_reader)):
            return True
        result = target_node.remoter.run("df -al | grep '/var/lib/scylla'")
        free_space_size = int(result.stdout.split()[3])
        occupy_space_size = int(free_space_size * 90 / 100)
        occupy_space_cmd = f'fallocate -l {occupy_space_size}K /var/lib/scylla/occupy_90percent.{time.time()}'
        LOGGER.debug('Cost 90% free space on /var/lib/scylla/ by {}'.format(occupy_space_cmd))
        try:
            target_node.remoter.sudo(occupy_space_cmd, verbose=True)
        except Exception as details:  # pylint: disable=broad-except  # noqa: BLE001
            LOGGER.warning(str(details))
        return bool(list(no_space_log_reader))

    wait.wait_for(func=approach_enospc,
                  timeout=300,
                  step=5,
                  text='Wait for new ENOSPC error occurs in database',
                  throw_exc=False
                  )


def clean_enospc_on_node(target_node, sleep_time):
    LOGGER.debug('Sleep {} seconds before releasing space to scylla'.format(sleep_time))
    time.sleep(sleep_time)

    LOGGER.debug('Delete occupy_90percent file to release space to scylla-server')
    target_node.remoter.sudo('rm -rf /var/lib/scylla/occupy_90percent.*')

    LOGGER.debug('Sleep a while before restart scylla-server')
    time.sleep(sleep_time / 2)
    target_node.restart_scylla_server()
    target_node.wait_db_up()


def parse_nodetool_listsnapshots(listsnapshots_output: str) -> defaultdict:
    """
    listsnapshots output:

        Snapshot Details:
        Snapshot name Keyspace name Column family name True size Size on disk
        1599414845162 system_schema keyspaces          0 bytes   71.71 KB
        1599414845162 system_schema scylla_tables      0 bytes   73.21 KB
        1599414845162 system_schema tables             0 bytes   81.48 KB
        1599414845162 system_schema columns            0 bytes   80.12 KB

        Total TrueDiskSpaceUsed: 0 bytes
    """
    snapshots_content = defaultdict(list)
    SnapshotDetails = namedtuple("SnapshotDetails", ["keyspace_name", "table_name"])
    for line in listsnapshots_output.splitlines():
        if line and not line.startswith('Snapshot') and not line.startswith('Total'):
            line_splitted = line.split()
            snapshots_content[line_splitted[0]].append(SnapshotDetails(line_splitted[1], line_splitted[2]))
    return snapshots_content


def convert_metric_to_ms(metric: str) -> float:
    """
    Convert metric value to ms and float.
    Expected values:
        "8.592961906s"
        "18.120703ms"
        "5.963775µs"
        "9h0m0.024080491s"
        "1m0.024080491s"
        "546431"
        "950µs"
        "30ms"
    """

    def _convert_to_ms(units, value):
        if not value:
            return 0

        if units == 'hour':
            return float(value) * 3600 * 1000
        elif units == 'min':
            return float(value) * 60 * 1000
        elif units == 's':
            return float(value) * 1000
        elif units == 'µs':
            return float(value) / 1000
        else:
            return float(value)

    pattern = r"^((?P<hour>\d+)h)?((?P<min>\d+)m)?(?P<sec>\d+\.?(\d+)?)(?P<units>s|ms|µs)?"
    try:
        found = re.match(pattern, metric)
        if found:
            parsed_values = found.groupdict()
            metric_converted = 0
            metric_converted += _convert_to_ms('hour', parsed_values['hour'])
            metric_converted += _convert_to_ms('min', parsed_values['min'])
            metric_converted += _convert_to_ms(parsed_values['units'], parsed_values['sec'])
        else:
            metric_converted = float(metric)
    except ValueError as ve:  # pylint: disable=invalid-name
        metric_converted = metric
        LOGGER.error("Value %s can't be converted to float. Exception: %s", metric, ve)
    return metric_converted


def _shorten_alpha_sequences(value: str, max_alpha_chunk_size: int) -> str:
    if not value:
        return value
    is_alpha = value[0].isalpha()
    num = 0
    output = ''
    for char in value:
        if is_alpha == char.isalpha():
            if is_alpha and num >= max_alpha_chunk_size:
                continue
            num += 1
            output += char
            continue
        output += char
        num = 1
    return output


def _shorten_sequences_in_string(value: Union[str, List[str]], max_alpha_chunk_size: int) -> str:
    chunks = []
    if isinstance(value, str):
        tmp = value.split('-')
    else:
        tmp = value
    for chunk in tmp:
        chunks.append(_shorten_alpha_sequences(chunk, max_alpha_chunk_size))
    return '-'.join(chunks)


def _string_max_chunk_size(value):
    return max([len(chunk) for chunk in value.split('-')])


def shorten_cluster_name(name: str, max_string_len: int):
    """
    Make an attempt to shorten cluster/any name so that it would fit into max_string_len limit
    If it can't make it that short, it will return original name
    Shortening is done in following manner:
    1. It split string by '-' and take out and preserve last chunk (supposedly short test id there)
    2. Array of chunks that is left it splits into sequences of digits and non-digits
    3. Next it goes over non-digit chunks and trims them from right side by 1 char
    4. On each trimming round it recombine name back in exact same order
    5. Check if resulted string has len less than max_string_len and return it if it does
    6. If trimming is not possible anymore it return original name

    Example:
        original name - longevity-scylla-operator-3h-gke-je-k8s-gke-cd86ad2b
        shorten name - lon-scy-ope-3h-gke-je-k8s-gke-cd86ad2b
    """
    max_alpha_chunk_size = _string_max_chunk_size(name)
    last_chunk = name.split('-')[-1]
    current = '-'.join(name.split('-')[0:-1])
    last_chunk_len = len(last_chunk)
    while len(current) + last_chunk_len + 1 > max_string_len and max_alpha_chunk_size > 0:
        current = _shorten_sequences_in_string(name.split('-')[0:-1], max_alpha_chunk_size)
        max_alpha_chunk_size -= 1
    if max_alpha_chunk_size == 0:
        return name
    return '-'.join([current, last_chunk])


def download_from_github(repo: str, tag: str, dst_dir: str):
    """
    Downloads files from github via http to the dst_dir directory
    """
    url = f'https://github.com/{repo}/archive/{tag}.zip'
    resp = requests.get(url, allow_redirects=True)
    if not resp.ok:
        raise RuntimeError(f"Failed to download {url}, result: {resp.content}")
    os.makedirs(dst_dir, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(io.BytesIO(resp.content), 'r') as zip_ref:
            zip_ref.extractall(tmpdir)
        base_dir = os.path.join(tmpdir, os.listdir(tmpdir)[0])
        for file in os.listdir(base_dir):
            os.rename(os.path.join(base_dir, file), os.path.join(dst_dir, file))


def walk_thru_data(data, path: str, separator: str = '/') -> Any:
    """Allows to get a value of an element in some data structure.

    Example from K8S API:
    object_dict = {
        "spec": {
            "automaticOrphanedNodeCleanup": True,
            "datacenter": {
                "name": "dc-1",
                racks: [...],
            },
        },
    }

    And to get the DC name out of it we can call this function like following:

        walk_thru_data(data=object_dict, path='spec.datacenter.name', separator=".")

    """
    current_value = data
    for name in path.split(separator):
        if current_value is None:
            return None
        if not name:
            continue
        if name[0] == '[' and name[-1] == ']':
            name = name[1:-1]  # noqa: PLW2901
        if name.isalnum() and isinstance(current_value, (list, tuple, set)):
            try:
                current_value = current_value[int(name)]
            except Exception:  # pylint: disable=broad-except  # noqa: BLE001
                current_value = None
            continue
        current_value = current_value.get(name, None)
    return current_value


def update_authenticator(nodes, authenticator='AllowAllAuthenticator', restart=True):
    """
    Update the authenticator of nodes, restart the nodes to make the change effective
    """
    for node in nodes:
        with node.remote_scylla_yaml() as scylla_yml:
            scylla_yml.authenticator = authenticator
        if restart:
            if authenticator == SASLAUTHD_AUTHENTICATOR:
                node.parent_cluster.use_ldap_authentication = True
            else:
                node.parent_cluster.use_ldap_authentication = False
            node.restart_scylla_server()
            node.wait_db_up()


def prepare_and_start_saslauthd_service(node):
    """
    Install and setup saslauthd service.
    """
    if node.distro.is_rhel_like:
        setup_script = dedent("""
            sudo yum install -y cyrus-sasl
            sudo systemctl enable saslauthd
            echo 'MECH=ldap' | sudo tee -a /etc/sysconfig/saslauthd
            sudo touch /etc/saslauthd.conf
        """)
    else:
        setup_script = dedent("""
            sudo apt-get install -o DPkg::Lock::Timeout=300 -y sasl2-bin
            sudo systemctl enable saslauthd
            echo -e 'MECHANISMS=ldap\nSTART=yes\n' | sudo tee -a /etc/default/saslauthd
            sudo touch /etc/saslauthd.conf
            sudo adduser scylla sasl  # to avoid the permission issue of unit socket
        """)
    node.remoter.run('bash -cxe "%s"' % setup_script)
    if node.parent_cluster.params.get('ldap_server_type') == LdapServerType.MS_AD:
        conf = node.get_saslauthd_ms_ad_config()
    else:
        conf = node.get_saslauthd_config()
    for key in conf.keys():
        node.remoter.run(f'echo "{key}: {conf[key]}" | sudo tee -a /etc/saslauthd.conf')
    with node.remote_scylla_yaml() as scylla_yml:
        scylla_yml.saslauthd_socket_path = '/run/saslauthd/mux'
    node.remoter.sudo('systemctl restart saslauthd')


def change_default_password(node, user='cassandra', password='cassandra'):
    """
    Default password of Role `cassandra` is same as username, MS-AD doesn't allow the weak password.
    Here we change password of `cassandra`, then the cassandra user can smoothly work in switching Authenticator.
    """
    node.run_cqlsh(f"ALTER ROLE '{user}' with password='{password}{DEFAULT_PWD_SUFFIX}'")
    node.parent_cluster.added_password_suffix = True


def make_threads_be_daemonic_by_default():
    """
    There is a problem with some code that uses Thread with no option to tweak it to be daemonic.
    Good example of it would be concurent.futures.thread.ThreadPoolExecutor
    Mostly this code do not pass daemon=True to __init__ and
    therefore threading.Thread pick up value of this flag from current_thread
    This code is to change this flag to True to make all threads daemonic by default
    @return:
    @rtype:
    """
    threading.current_thread()._daemonic = True  # pylint: disable=protected-access


def clear_out_all_exit_hooks():
    """
    Some thread-related code is using threading._register_atexit to hook to python program exit
    in order teardown gracefully as result test can halt at the end for any period of time, even for days.
    To avoid that we clear it out to be sure that nothing is hooked and test is terminated right after
      teardown.
    """
    threading._threading_atexits.clear()  # pylint: disable=protected-access


def validate_if_scylla_load_high_enough(start_time, wait_cpu_utilization, prometheus_stats,
                                        event_severity=Severity.ERROR, instance=None):
    end_time = int(time.time())
    scylla_load = prometheus_stats.get_scylla_reactor_utilization(start_time=start_time, end_time=end_time,
                                                                  instance=instance)

    if scylla_load < wait_cpu_utilization:
        CpuNotHighEnoughEvent(message=f"Load {scylla_load} isn't high enough(expected at least {wait_cpu_utilization})."
                                      " The test results may be not correct.",
                              severity=event_severity).publish()
        return False

    return True


class RemoteTemporaryFolder:
    def __init__(self, node):
        self.node = node
        self.folder_name = ""

    def __enter__(self):
        result = self.node.remoter.run('mktemp -d')
        self.folder_name = result.stdout.strip()
        return self

    def __exit__(self, exit_type, value, _traceback):
        # remove the temporary folder as `sudo` to cover the case when the folder owner was changed during test
        self.node.remoter.sudo(f'rm -rf {self.folder_name}')


duration_pattern = re.compile(r'(?P<hours>[\d]*)h|(?P<minutes>[\d]*)m|(?P<seconds>[\d]*)s')


def time_period_str_to_seconds(time_str: str) -> int:
    """Transforms duration string into seconds int. e.g. 1h -> 3600, 1h22m->4920 or 10m->600"""
    return sum([int(g[0] or 0) * 3600 + int(g[1] or 0) * 60 + int(g[2] or 0) for g in duration_pattern.findall(time_str)])


def sleep_for_percent_of_duration(duration: int, percent: int, min_duration: int, max_duration: int):
    """Waits the percentage of a duration in seconds, with a minimum and maximum duration (min < duration * percentage < max)"""
    duration = int(duration * percent / 100)
    duration = max(min(duration, max_duration), min_duration)
    LOGGER.debug("Sleeping for %s seconds", duration)
    time.sleep(duration)


def get_keyspace_partition_ranges(node, keyspace: str):
    result = node.run_nodetool("describering", keyspace)
    if not result.stdout:
        return None

    ranges_as_list = re.findall(r'^\s*TokenRange\((.*)\)\s*$', result.stdout, re.MULTILINE)
    if not ranges_as_list:
        raise ValueError(f"No TokenRange() found in describering: {result.stdout}")

    return [describering_parsing(one_range) for one_range in ranges_as_list]


def keyspace_min_max_tokens(node, keyspace: str):
    ranges = get_keyspace_partition_ranges(node, keyspace)
    if not ranges:
        return None, None

    min_token = min([token['start_token'] for token in ranges])
    max_token = max([token['end_token'] for token in ranges])
    return min_token, max_token


def describering_parsing(describering_output):
    def _list2dic(attr_list, heads_to_dict):
        res = {}
        for ind, attr in enumerate(heads_to_dict):
            res[attr] = attr_list[ind].strip()
        return res

    found_attributes = re.findall(r'^\s*start_token:(-?\d+), end_token:(-?\d+), endpoints:\[([\d\., ]+)\], '
                                  r'rpc_endpoints:\[([\d\., ]+)\], endpoint_details:\[(.*)\]\s*$',
                                  describering_output, re.MULTILINE)
    heads = ['start_token', 'end_token', 'endpoints', 'rpc_endpoints']
    result = {}
    assert found_attributes, "Wrong format of token range: " + describering_output
    for index, attribute in enumerate(heads):
        attr_value = found_attributes[0][index].strip()
        result[attribute] = int(attr_value) if "token" in attribute else attr_value
        result["details"] = [_list2dic(attr_list, ['host', 'datacenter', 'rack']) for attr_list in
                             re.findall(r'EndpointDetails\(host:([\d\.,]+), datacenter:([^,]+), rack:([^\)]+)\),?',
                                        found_attributes[0][4])]
    return result


@contextmanager
def SoftTimeoutContext(timeout: int, operation: str):  # pylint: disable=invalid-name
    """Publish SoftTimeoutEvent with operation info in case of duration > timeout"""
    start_time = time.time()
    yield
    duration = time.time() - start_time
    if duration > timeout:
        SoftTimeoutEvent(operation=operation, soft_timeout=timeout,
                         duration=duration).publish_or_dump()


def raise_exception_in_thread(thread: threading.Thread, exception_type: Type[BaseException]):
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_ulong(thread.ident), ctypes.py_object(exception_type))
    LOGGER.debug("PyThreadState_SetAsyncExc: return [%s]", res)


def list_placement_groups_aws(tags_dict=None, region_name=None, available=False, group_as_region=False, verbose=False):
    """
        list all placement groups with specific tags AWS

        :param tags_dict: key-value pairs used for filtering
        :param region_name: name of the region to list
        :param available: get all available placement groups
        :param group_as_region: if True the results would be grouped into regions
        :param verbose: if True will log progress information

        :return: instances dict where region is a key
        """
    placement_groups = {}
    aws_regions = [region_name] if region_name else all_aws_regions()

    def get_placement_groups(region):
        if verbose:
            LOGGER.info('Going to list aws region "%s"', region)
        time.sleep(random.random())
        client: EC2Client = boto3.client('ec2', region_name=region)
        custom_filter = []
        if tags_dict:
            custom_filter = [{'Name': 'tag:{}'.format(key),
                              'Values': value if isinstance(value, list) else [value]}
                             for key, value in tags_dict.items()]
        response = client.describe_placement_groups(Filters=custom_filter)
        placement_groups[region] = list(response['PlacementGroups'])

        if verbose:
            LOGGER.info("%s: done [%s/%s]", region, len(list(placement_groups.keys())), len(aws_regions))

    ParallelObject(aws_regions, timeout=100, num_workers=len(aws_regions)
                   ).run(get_placement_groups, ignore_exceptions=True)

    for curr_region_name in placement_groups:
        if available:
            placement_groups[curr_region_name] = [
                i for i in placement_groups[curr_region_name] if i['State'] == 'available']
        else:
            placement_groups[curr_region_name] = [i for i in placement_groups[curr_region_name]
                                                  if not i['State'] == 'deleted']
    if not group_as_region:
        placement_groups = list(itertools.chain(*list(placement_groups.values())))  # flatten the list of lists
        total_items = len(placement_groups)
    else:
        total_items = sum([len(value) for _, value in placement_groups.items()])

    if verbose:
        LOGGER.info("Found total of {} instances.".format(total_items))

    return placement_groups


def skip_optional_stage(stage_names: str | list[str]) -> bool:
    """
    Checks if the given test stage(s) is skipped for execution

    :param stage_names: str or list, name of the test stage(s)
    :return: bool
    """
    # making import here, to work around circular import issue
    from sdcm.cluster import TestConfig
    stage_names = stage_names if isinstance(stage_names, list) else [stage_names]
    skip_test_stages = TestConfig().tester_obj().skip_test_stages
    skipped_stages = [stage for stage in stage_names if skip_test_stages[stage]]

    if skipped_stages:
        skipped_stages_str = ', '.join(skipped_stages)
        LOGGER.warning("'%s' test stage(s) is disabled.", skipped_stages_str)
        return True
    return False


def parse_python_thread_command(cmd: str) -> dict:
    """
    Parses a command string into a dictionary of options

    :param cmd: str, the command string to parse

    :return: dict, dictionary of options' name-value pairs.
    """
    options = {}
    tokens = shlex.split(cmd)
    tokens_iter = iter(tokens)

    command_name = next(tokens_iter, None)

    if command_name is None:
        LOGGER.error("Empty command string is provided.")
        return options

    for token in tokens_iter:
        if token.startswith('-'):
            if '=' in token:
                # Option and value are in the same token ('-option=value')
                option, value = token.split('=', 1)
                option_name = option.lstrip('-')
                options[option_name] = value
            else:
                # Option without separator; may be followed by its value
                option_name = token.lstrip('-')
                next_token = next(tokens_iter, None)
                if next_token and not next_token.startswith('-'):
                    # Next token is the value for the current option
                    options[option_name] = next_token
                else:
                    # Option is a flag
                    options[option_name] = True
                    if next_token:
                        # Next token is another option; re-insert it into the iterator
                        tokens_iter = iter([next_token] + list(tokens_iter))

    return options
