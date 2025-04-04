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
# Copyright (c) 2024 ScyllaDB

from datetime import datetime
from logging import getLogger
from functools import wraps

import boto3
import requests
from sdcm.utils.common import ParallelObject
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.sct_events import Severity


logger = getLogger(__name__)


def get_repo_urls(start_date: datetime, end_date: datetime, is_enterprise: bool = False):
    bucket = 'downloads.scylladb.com'
    logger.debug("getting repo urls for is_enterprise: %s date range: %s - %s", is_enterprise, start_date, end_date)
    if is_enterprise:
        prefix = 'unstable/scylla-enterprise/enterprise/deb/unified/'
        suffix = 'scylladb-enterprise/scylla.list'
    else:
        prefix = 'unstable/scylla/master/deb/unified/'
        suffix = 'scylladb-master/scylla.list'
    response = boto3.client('s3').list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
    dates = [obj['Prefix'].rsplit('/', 2)[-2]
             for obj in response.get("CommonPrefixes", []) if obj['Prefix'].endswith("Z/")]
    repos = [f"https://{bucket}/{prefix}{date}/{suffix}" for date in dates if
             start_date <= datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ') <= end_date]
    repos = [repo for repo in repos if requests.head(repo).ok]  # filter out non-existing repos
    logger.debug("Repos to bisect: %s", repos)
    return repos


def bisect_test(test_func):
    """
    decorator for bisecting tests.

    Wrap a test function with this decorator and supply `bisect_start_date` and `bisect_end_date` to sct_config class to bisect it.
    Before bisecting, test function will be called once with the initial binaries and must set the reference value
    (`bisect_ref_value` attribute of tester class, int type).
    Test function should update `bisect_result_value` attribute (int type) of tester class on each run.
    On each iteration, the decorator will update the binaries according to bisection algorithm
    and `tester.bisect_result_value` >= `bisect_ref_value` comparison result - if is true, means result is ok (no regression).
    """

    @wraps(test_func)
    def wrapper(*args, **kwargs):  # noqa: PLR0914
        tester_obj = args[0]
        start_date = tester_obj.params.get('bisect_start_date')
        test_func(*args, **kwargs)
        if not start_date:
            # no bisect start date, no need to bisect
            return
        cluster = tester_obj.db_cluster
        tester_obj.create_stats = False

        def update_binaries(node):
            node.stop_scylla()
            cluster._scylla_install(node)

        end_date = tester_obj.params.get('bisect_end_date')
        bisect_start_date = datetime.strptime(start_date, '%Y-%m-%d')
        bisect_end_date = datetime.strptime(end_date, '%Y-%m-%d') if end_date else datetime.today()
        repo_urls = get_repo_urls(bisect_start_date, bisect_end_date, is_enterprise=cluster.nodes[0].is_enterprise)

        low, high = 0, len(repo_urls) - 1
        last_good_version = 'unknown'
        first_bad_version = 'unknown'
        version = 'unknown'
        while low <= high:
            tester_obj.stop_timeout_thread()
            tester_obj._init_test_timeout_thread()
            mid = (low + high) // 2
            repo_url = repo_urls[mid]
            tester_obj.params['scylla_repo'] = repo_url
            logger.info("Updating binaries from repo: %s", repo_url)
            parallel_object = ParallelObject(cluster.nodes, num_workers=len(cluster.nodes), timeout=500)
            try:
                parallel_object.run(update_binaries)
                for idx, node in enumerate(cluster.nodes):
                    logger.info('starting updated node: %s', node.name)
                    if idx == 0:  # make first node a seed to bootstrap it properly after full cluster cleanup
                        with node.remote_scylla_yaml() as scylla_yml:
                            current_seed_provider = scylla_yml.seed_provider
                            original_seed_provider = current_seed_provider.copy()
                            current_seed_provider[0].parameters = [{"seeds": str(node.private_ip_address)}]
                            scylla_yml.seed_provider = current_seed_provider
                        node.start_scylla()
                        node.wait_db_up()
                        # and recover seeds to original state
                        with node.remote_scylla_yaml() as scylla_yml:
                            scylla_yml.seed_provider = original_seed_provider
                    else:
                        node.start_scylla()
                        node.wait_db_up()
                    version = node.get_scylla_binary_version()
                    if not version:
                        raise ValueError('failed to get version from node: ', node.name)
                    logger.info('successfully updated binaries to version: %s', version)

            except Exception as exc:  # noqa: BLE001
                logger.warning('error during upgrade: %s \n verifying next closest version.', exc)
                del repo_urls[mid]
                high -= 1
                continue

            TestFrameworkEvent(source="bisection", message=f"Updated Scylla binaries to: {version}",
                               severity=Severity.WARNING).publish_or_dump()
            test_func(*args, **kwargs)
            logger.info("Evaluating regression: %s >= %s", tester_obj.bisect_result_value, tester_obj.bisect_ref_value)
            if tester_obj.bisect_result_value >= tester_obj.bisect_ref_value:
                last_good_version = version
                logger.info("Version %s evaluates to False -> it's ok. Checking later versions.", version)
                low = mid + 1
            else:
                first_bad_version = version
                logger.info("Version %s evaluates to True -> it's bad. Checking earlier versions.", version)
                high = mid - 1
        logger.info("Last good version: %s, first bad version: %s", last_good_version, first_bad_version)

    return wrapper
