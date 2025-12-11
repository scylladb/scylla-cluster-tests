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
# Copyright (c) 2021 ScyllaDB

import re
import typing
from datetime import datetime
import logging

from sdcm.es import ES
from test_lib.utils import get_class_by_path
from .base import ClassBase, __DEFAULT__
from .metrics import ScyllaTestMetrics


LOGGER = logging.getLogger(__name__)


ES_LUCENE_ESCAPE_REGEXP = re.compile(r'([^0-9a-zA-Z_.])')


class DateClassBase(ClassBase):
    value: datetime.date = None

    def load_from_es_data(self, es_data):
        raise NotImplementedError()

    def save_to_es_data(self):
        raise NotImplementedError()

    def save_to_report_data(self):
        if self.value is None:
            return None
        return self.value.strftime('%Y-%m-%d')

    def as_string(self, datetime_format='%Y-%m-%d'):
        if self.value is None:
            return None
        return self.value.strftime(datetime_format)

    def is_valid(self):
        return self.value is not None

    def _check_if_comparable(self, other):
        if type(self) is not type(other):
            raise ValueError(f"{self.__class__.__name__} can be compared only to same class")
        if not self.is_valid():
            raise ValueError(f"Can't compare {self.__class__.__name__} if it is not valid")
        if not other.is_valid():
            raise ValueError(f"Can't compare {self.__class__.__name__} to not valid instance")

    __hash__ = False

    def __le__(self, other):
        self._check_if_comparable(other)
        return self.value <= other.date

    def __lt__(self, other):
        self._check_if_comparable(other)
        return self.value < other.value

    def __gt__(self, other):
        self._check_if_comparable(other)
        return self.value > other.value

    def __ge__(self, other):
        self._check_if_comparable(other)
        return self.value >= other.value

    def __eq__(self, other):
        self._check_if_comparable(other)
        return self.value == other.value


class FloatDateClassBase(DateClassBase):
    def load_from_es_data(self, es_data):
        try:
            self.value = datetime.utcfromtimestamp(es_data)
        except ValueError:
            pass

    def save_to_es_data(self):
        if self.value is None:
            return None
        return self.value.timestamp()


class StrDateClassBase(DateClassBase):
    value: datetime.date = None
    _format = None

    def load_from_es_data(self, es_data):
        try:
            self.value = datetime.strptime(es_data, self._format)
        except ValueError:
            pass

    def save_to_es_data(self):
        if self.value is None:
            return None
        return self.value.strftime(self._format)


class SoftwareVersionDate(StrDateClassBase):
    _format = "%Y%m%d"


class SoftwareVersion(ClassBase):
    as_string = None

    def load_from_es_data(self, es_data):
        try:
            self.as_string = es_data
        except ValueError:
            pass

    def save_to_es_data(self):
        return self.as_string

    @property
    def as_int(self):
        if self.as_string == '666.development' or self.as_string.endswith('.dev'):
            return (100 ** 5) * 10
        version_parts = self.as_string.split('.')
        idx = 100**5
        output = 0
        for version_part in version_parts:
            if version_part.isdecimal():
                output += idx * int(version_part)
            elif 'rc' in version_part:
                output += (idx//100) * int(version_part.replace('rc', ''))
            else:
                raise ValueError(f"Can't parse version string {self.as_string}")
            idx = idx // 100
        return output

    @property
    def major_as_int(self):
        if self.as_string == '666.development' or self.as_string.endswith('.dev'):
            return (100 ** 5) * 10
        version_parts = self.as_string.split('.')
        idx = 100**5
        output = 0
        if len(version_parts) >= 2:
            for version_part in version_parts[:-1]:
                if version_part.isdecimal():
                    output += idx * int(version_part)
                else:
                    raise ValueError(f"Can't parse version string {self.as_string}")
                idx = idx // 100
        return output


class SoftwareVersionInfoBase(ClassBase):
    name = None
    version: SoftwareVersion = None
    date: SoftwareVersionDate = None
    commit: str = None
    _es_data_mapping = {
        'commit': 'commit_id'
    }

    def is_valid(self):
        return self.version and self.date and self.date.is_valid()

    def is_same_kind(self, other):
        return self.__class__ is other.__class__ and other.name == self.name

    def _check_if_can_compare(self, other):
        if not self.is_same_kind(other):
            raise ValueError(f"Can't compare {self.__class__.__name__} with other type")
        if not self.is_valid() or not other.is_valid():
            raise ValueError(f"Can't compare {self.__class__.__name__}, both should be valid")

    __hash__ = False

    def __le__(self, other):
        self._check_if_can_compare(other)
        self_version = self.version.as_int
        other_version = other.version.as_int
        if self_version != other_version:
            return self_version <= other_version
        return self.date <= other.date

    def __lt__(self, other):
        self._check_if_can_compare(other)
        self_version = self.version.as_int
        other_version = other.version.as_int
        if self_version != other_version:
            return self_version < other_version
        return self.date < other.date

    def __gt__(self, other):
        self._check_if_can_compare(other)
        self_version = self.version.as_int
        other_version = other.version.as_int
        if self_version != other_version:
            return self_version > other_version
        return self.date > other.date

    def __ge__(self, other):
        self._check_if_can_compare(other)
        self_version = self.version.as_int
        other_version = other.version.as_int
        if self_version != other_version:
            return self_version >= other_version
        return self.date >= other.date

    def __eq__(self, other):
        self._check_if_can_compare(other)
        self_version = self.version.as_int
        other_version = other.version.as_int
        if self_version != other_version:
            return False
        if self.date != other.date:
            return False
        return True


class ScyllaVersionInfo(SoftwareVersionInfoBase):
    name = 'scylla-server'


class ScyllaEnterpriseVersionInfo(SoftwareVersionInfoBase):
    name = 'scylla-enterprise-server'


class SoftwareVersions(ClassBase):
    _es_data_mapping = {
        'scylla_server': 'scylla-server',
        'scylla_enterprise_server': 'scylla-enterprise-server',
    }
    scylla_server: ScyllaVersionInfo = None
    scylla_enterprise_server: ScyllaEnterpriseVersionInfo = None

    @property
    def scylla_server_any(self) -> typing.Union[ScyllaVersionInfo, ScyllaEnterpriseVersionInfo]:
        return self.scylla_server if self.scylla_server else self.scylla_enterprise_server

    def is_valid(self):
        for data_name in self.__annotations__.keys():
            default = getattr(self.__class__, data_name)
            value = getattr(self, data_name, None)
            if value is default:
                continue
            if isinstance(value, ClassBase):
                if not value.is_valid():
                    continue
            return True
        return False


class SctClusterBase(ClassBase):
    nodes: int = None
    type: str = None
    gce_type: str = None
    ami_id: str = None

    __hash__ = False

    def __eq__(self, other):
        for name in ['nodes', 'type', 'gce_type']:
            if getattr(self, name, None) != getattr(other, name, None):
                return False
        return True

    def is_valid(self):
        return bool(self.nodes and (self.type or self.gce_type))


class LoaderCluster(SctClusterBase):
    _es_data_mapping = {
        'nodes': 'n_loaders',
        'type': 'instance_type_loader',
        'gce_type': 'gce_instance_type_db',
        'ami_id': 'ami_id_db_scylla'
    }


class DbCluster(SctClusterBase):
    _es_data_mapping = {
        'nodes': 'n_db_nodes',
        'type': 'instance_type_db',
        'gce_type': 'gce_instance_type_db',
        'ami_id': 'ami_id_db_scylla'
    }


class MonitorCluster(SctClusterBase):
    _es_data_mapping = {
        'nodes': 'n_monitor_nodes',
        'type': 'instance_type_monitor',
        'gce_type': 'gce_instance_type_monitor',
        'ami_id': 'ami_id_db_scylla'
    }


class TestCompleteTime(StrDateClassBase):
    _format = "%Y-%m-%d %H:%M"


class TestStartDate(FloatDateClassBase):
    pass


class CassandraStressRateInfo(ClassBase):
    threads: int = None
    throttle: int = None
    _es_data_mapping = {
        'treads': 'rate threads',
        'throttle': 'throttle threads',
    }


class CassandraStressCMDInfo(ClassBase):
    mode: str = None
    no_warmup: bool = None
    ops: str = None
    raw_cmd: str = None
    port: str = None
    profile: str = None
    cl: str = None
    command: str = None
    n: int = None
    rate: CassandraStressRateInfo = None
    _es_data_mapping = {
        'no_warmup': 'no-warmup',
        'rate': ''
    }

    def is_valid(self):
        return bool(self.raw_cmd) and self.command


class GeminiCMDInfo(ClassBase):
    mode: str = None
    no_warmup: bool = None
    ops: str = None
    raw_cmd: str = None
    port: str = None
    profile: str = None
    cl: str = None
    command: str = None
    n: int = None
    rate: CassandraStressRateInfo = None
    _es_data_mapping = {
        'no_warmup': 'no-warmup',
        'rate': ''
    }

    def is_valid(self):
        return bool(self.raw_cmd) and self.command


class JenkinsRunInfo(ClassBase):
    job_name: str = None
    job_url: str = None


class TestResultClass(ClassBase):
    _es_data_mapping = {
        'metrics': '_source',
        'subtest_name': '_source.test_details.sub_type',
        'test_id': '_id',
        'software': '_source.versions',
        'grafana_screenshots': '_source.test_details.grafana_screenshots',
        'es_index': '_index',
        'main_test_id': '_source.test_details.test_id',
        'setup_details': '_source.setup_details',
        'test_name': '_source.test_details.test_name',
        'db_cluster': '_source.setup_details',
        'loader_cluster': '_source.setup_details',
        'monitor_cluster': '_source.setup_details',
        'complete_time': '_source.test_details.time_completed',
        'start_time': '_source.test_details.start_time',
        'jenkins': '_source.test_details',
        'preload_cassandra_stress': '_source.test_details.preload-cassandra-stress',
        'cassandra_stress': '_source.test_details.cassandra-stress',
        'started_by': '_source.test_details.started_by',  # Never there
        'scylla_repo': '_source.setup_details.scylla_repo_m',
        'scylla_repo_uuid': '_source.setup_details.scylla_repo_uuid',  # Never there
        'scylla_mgmt_address': '_source.setup_details.scylla_mgmt_address',
        'backend': '_source.setup_details.cluster_backend',
        'ostype': '_source.setup_details.cluster_backend.ostype',  # Never there
        'gemini_version': '_source.setup_details.gemini_version',
        'status': '_source.status'
    }
    _es_field_indexes = ['_id']
    test_id: str = None
    main_test_id: str = None
    backend: str = None
    db_cluster: DbCluster = None
    loader_cluster: LoaderCluster = None
    monitor_cluster: MonitorCluster = None
    metrics: ScyllaTestMetrics = None
    subtest_name: str = None
    software: SoftwareVersions = None
    grafana_screenshots: list = []
    test_name: str = None
    es_index: str = None
    started_by: str = None
    jenkins: JenkinsRunInfo = None
    setup_details: dict = None
    preload_cassandra_stress: CassandraStressCMDInfo = None
    cassandra_stress: CassandraStressCMDInfo = None
    complete_time: TestCompleteTime = None
    start_time: TestStartDate = None

    scylla_repo: str = None
    scylla_repo_uuid: str = None
    scylla_mgmt_address: str = None
    ostype: str = None
    gemini_version: str = None
    status: str = None
    remark = ''

    def __init__(self, es_data: dict, **kwargs):
        self._es_data = es_data
        super().__init__(es_data, **kwargs)

    def is_valid(self):
        return self.software and self.software.is_valid() and \
            self.setup_details and self.test_id and \
            self.monitor_cluster and self.monitor_cluster.is_valid() and \
            self.loader_cluster and self.loader_cluster.is_valid() and \
            self.db_cluster and self.db_cluster.is_valid()

    def is_gce(self):
        return self.setup_details and self.setup_details.get('cluster_backend', None) == 'gce'

    @classmethod
    def _get_es_filters(cls, depth=2):
        tmp = []
        for es_filter in cls._get_all_es_data_mapping().values():
            es_filter = '.'.join(es_filter.split('.')[:depth])  # noqa: PLW2901
            if es_filter not in tmp:
                tmp.append(es_filter)
        return ['hits.hits.' + es_filter for es_filter in tmp]

    @classmethod
    def _get_es_query_from_instance_data(cls, instance_data: dict):
        mappings = cls._get_all_es_data_mapping()
        es_data = {}
        # TBD: _get_all_data_mapping overwrites es data path when _data_mapping value is list
        for data_path, data_value in instance_data.items():
            es_data_path = mappings.get(data_path, None)
            if es_data_path is None:
                raise ValueError(
                    f"Unknown data path {data_path} only following are known:\n" + '\n'.join(mappings.keys()))
            es_data[es_data_path] = data_value
        return cls._get_es_query_from_es_data(es_data)

    @classmethod
    def _get_es_query_from_es_data(cls, es_data: dict):
        filters = []
        for es_data_path, data_value in es_data.items():
            es_data_path = es_data_path.split('.')  # noqa: PLW2901
            if es_data_path[0] == '_source':
                es_data_path = es_data_path[1:]  # noqa: PLW2901
            es_data_path = '.'.join(es_data_path)  # noqa: PLW2901
            es_data_path = cls._escape_filter_key(es_data_path)  # noqa: PLW2901
            if isinstance(data_value, str) and es_data_path not in cls._es_field_indexes and data_value != '*':
                filters.append(f'{es_data_path}.keyword: \"{data_value}\"')
            elif isinstance(data_value, bool):
                filters.append(f'{es_data_path}: {str(data_value).lower()}')
            else:
                filters.append(f'{es_data_path}: {data_value}')
        return ' AND '.join(filters)

    @staticmethod
    def _escape_filter_key(filter_key):
        return ES_LUCENE_ESCAPE_REGEXP.sub(r'\\\1', filter_key)

    def _get_es_query_from_self(self, data_path_patterns: list):
        """
        Builds ES query from self parameters using list of data path patterns from data_path_patterns
            data pattern can have some_attr.*, which tells to build query using every data point from attribute some_attr
        """
        output = self._get_es_data_path_and_values_from_patterns(data_path_patterns, flatten=True)
        return self._get_es_query_from_es_data(output)

    @classmethod
    def get_by_params(cls, es_index=es_index, **params):
        es_query = cls._get_es_query_from_instance_data(params)
        filter_path = cls._get_es_filters()
        try:
            es_data = ES().search(
                index=es_index,
                q=es_query,
                size=10000,
                filter_path=filter_path,
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Unable to find ES data: %s", exc)
            es_data = None

        if not es_data:
            return []
        es_data = es_data.get('hits', {}).get('hits', {})
        if not es_data:
            return []
        return [cls(es_data=es_test_data) for es_test_data in es_data]

    @classmethod
    def get_by_test_id(cls, test_id, es_index):
        return cls.get_by_params(es_index=es_index, test_id=test_id)

    @classmethod
    def get_metric_class(cls, metric_path, default=__DEFAULT__):
        if default is __DEFAULT__:
            return get_class_by_path(cls, metric_path)
        return get_class_by_path(cls, metric_path, default)

    @staticmethod
    def gen_kibana_dashboard_url(
            dashboard_path="app/kibana#/dashboard/03414b70-0e89-11e9-a976-2fe0f5890cd0?_g=()"
    ):
        return "%s/%s" % (ES().conf.get('kibana_url'), dashboard_path)

    def get_subtests(self):
        return self.get_by_params(es_index=self.es_index, main_test_id=self.test_id, subtest_name='*')

    def get_same_tests_query(self):
        list_of_attributes = [
            'loader_cluster.type',
            'loader_cluster.gce_type',
            'loader_cluster.nodes',
            'db_cluster.type',
            'db_cluster.gce_type',
            'db_cluster.nodes'
        ]
        if self.jenkins and self.jenkins.is_valid():
            list_of_attributes.append('jenkins.job_name')
        if self.test_name:
            list_of_attributes.append('test_name')
        if self.subtest_name:
            list_of_attributes.append('subtest_name')
        if self.preload_cassandra_stress and self.preload_cassandra_stress.is_valid():
            list_of_attributes.append('preload_cassandra_stress.*')
        if self.cassandra_stress and self.cassandra_stress.is_valid():
            list_of_attributes.extend([
                'cassandra_stress.mode',
                'cassandra_stress.no_warmup',
                'cassandra_stress.ops',
                'cassandra_stress.profile',
                'cassandra_stress.cl',
                'cassandra_stress.command',
                'cassandra_stress.n',
                'cassandra_stress.rate.*',
            ])
        return self._get_es_query_from_self(list_of_attributes)

    def get_prior_tests(self, filter_path=None) -> typing.List['TestResultClass']:
        output = []
        try:
            es_query = self.get_same_tests_query()
            es_result = ES().search(
                index=self._es_data['_index'],
                q=es_query,
                size=10000,
                filter_path=filter_path,
            )
            es_result = es_result.get('hits', {}).get('hits', None) if es_result else None
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Unable to find ES data: %s", exc)
            es_result = None

        if not es_result:
            return output
        for es_data in es_result:
            test = TestResultClass(es_data)
            output.append(test)
        return output
