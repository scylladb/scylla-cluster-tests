import random
import logging
import re

from typing import List, Tuple
from abc import abstractmethod, ABC
from collections import OrderedDict

LOGGER = logging.getLogger(__name__)


class TopPartitionCmd(ABC):

    def __init__(self, ks_cf_list: List[str]):
        self.ks_cf_list = ks_cf_list
        self._built_args = {}

    @staticmethod
    def _parse_toppartitions_output(output: str) -> dict:
        """parsing output of toppartitions

        input format stored in output parameter:
        WRITES Sampler:
          Cardinality: ~10 (15 capacity)
          Top 10 partitions:
            Partition     Count       +/-
            (ks0:cf3) 9        11         0
            (ks0:cf3) 0         1         0
            (ks0:cf3) 1         1         0

        READS Sampler:
          Cardinality: ~10 (256 capacity)
          Top 3 partitions:
            Partition     Count       +/-
            (ks0:cf3) 0         3         0
            (ks0:cf3) 1         3         0
            (ks0:cf3) 2         3         0
            (ks0:cf3) 3         2         0

        return Dict:
        {
            'READS': {
                'toppartitions': '10',
                'partitions': OrderedDict('(ks0:cf3) 0': {'count': '1', 'margin': '0'},
                                          '(ks0:cf3) 1': {'count': '1', 'margin': '0'},
                                          '(ks0:cf3) 2': {'count': '1', 'margin': '0'}),
                'cardinality': '10',
                'capacity': '256',
            },
            'WRITES': {
                'toppartitions': '10',
                'partitions': OrderedDict('(ks0:cf3) 10': {'count': '1', 'margin': '0'},
                                          '(ks0:cf3) 11': {'count': '1', 'margin': '0'},
                                          '(ks0:cf3) 21': {'count': '1', 'margin': '0'}),
                'cardinality': '10',
                'capacity': '256',
                'sampler': 'WRITES'
            }
        }


        Arguments:
            output {str} -- stdout of nodetool topparitions command

        Returns:
            dict -- result of parsing
        """
        pattern1 = r"(?P<sampler>[A-Z]+)\sSampler:\W+Cardinality:\s~(?P<cardinality>[0-9]+)\s\((?P<capacity>[0-9]+)\scapacity\)\W+Top\s(?P<toppartitions>[0-9]+)\spartitions:"
        pattern2 = r"(?P<partition>\([\w:]+\)\s[\w:]+)\s+(?P<count>[\d]+)\s+(?P<margin>[\d]+)"
        toppartitions = {}
        for out in output.strip().split('\n\n'):
            partition = OrderedDict()
            sampler_data = re.search(pattern1, out, re.MULTILINE)
            assert sampler_data, f"Pattern:{pattern1} are not matched on string:\n {out}"
            sampler_data = sampler_data.groupdict()
            partitions = re.findall(pattern2, out, re.MULTILINE)
            LOGGER.debug("Next list of top partitions are found %s", partitions)
            for val in partitions:
                partition.update({val[0]: {'count': val[1], 'margin': val[2]}})
            sampler_data.update({'partitions': partition})
            toppartitions[sampler_data.pop('sampler')] = sampler_data
        return toppartitions

    def verify_output(self, output: str):
        toppartition_result = self._parse_toppartitions_output(output)
        for _sampler in self._built_args['samplers'].split(','):
            sampler = _sampler.upper()
            assert sampler in toppartition_result, "{} sampler not found in result".format(sampler)
            assert toppartition_result[sampler]['toppartitions'] == self._built_args['toppartition'], \
                "Wrong expected and actual top partitions number for {} sampler".format(sampler)
            assert toppartition_result[sampler]['capacity'] == self._built_args['capacity'], \
                "Wrong expected and actual capacity number for {} sampler".format(sampler)
            assert len(toppartition_result[sampler]['partitions'].keys()) <= int(self._built_args['toppartition']), \
                "Wrong number of requested and expected toppartitions for {} sampler".format(sampler)

    @abstractmethod
    def _filter_ks_cf(self) -> List[Tuple[str, str]]:
        ...

    @abstractmethod
    def get_cmd_args(self) -> str:
        ...

    @abstractmethod
    def generate_cmd_arg_values(self):
        ...


class NewApiTopPartitionCmd(TopPartitionCmd):
    def generate_cmd_arg_values(self):
        filtered_ks_cf = self._filter_ks_cf()

        self._built_args = {
            'toppartition': str(random.randint(5, 20)),
            'samplers': random.choice(['writes', 'reads', 'writes,reads']),
            'capacity': str(random.randint(100, 1024)),
            'duration': str(random.randint(1000, 10000)),
            'ks_filters': ",".join({ks for ks, _ in filtered_ks_cf}),
            'cf_filters': ','.join([f"{ks}:{cf}" for ks, cf in filtered_ks_cf])
        }

    def _filter_ks_cf(self) -> List[Tuple[str, str]]:
        filter_ks_cf = []
        try:
            for _ in range(random.randint(1, len(self.ks_cf_list))):
                keyspace, table_name = random.choice(self.ks_cf_list).split('.', maxsplit=1)
                filter_ks_cf.append((keyspace, table_name))
        except IndexError as details:
            LOGGER.error('Keyspace and ColumnFamily are not found %s.', self.ks_cf_list)
            LOGGER.debug('Error during choosing keyspace and column family %s', details)
            raise Exception('Keyspace and ColumnFamily are not found. \n{}'.format(details)) from IndexError

        return filter_ks_cf

    def get_cmd_args(self) -> str:
        return "--ks-filters {ks_filters} --cf-filters {cf_filters} -s {capacity} -k {toppartition} -a {samplers} -d {duration}".format(
            **self._built_args)


class OldApiTopPartitionCmd(TopPartitionCmd):
    def generate_cmd_arg_values(self):
        ks, cf = self._filter_ks_cf()[0]

        self._built_args = {
            'toppartition': str(random.randint(5, 20)),
            'samplers': random.choice(['writes', 'reads', 'writes,reads']),
            'capacity': str(random.randint(100, 1024)),
            'duration': str(random.randint(1000, 10000)),
            'ks': ks,
            'cf': cf,
        }

    def _filter_ks_cf(self) -> List[Tuple[str, str]]:
        try:
            keyspace, table_name = random.choice(self.ks_cf_list).split('.', maxsplit=1)
            return [(keyspace, table_name)]
        except IndexError as details:
            LOGGER.error('Keyspace and ColumnFamily are not found %s.', self.ks_cf_list)
            LOGGER.debug('Error during choosing keyspace and column family %s', details)
            raise Exception('Keyspace and ColumnFamily are not found. \n{}'.format(details)) from IndexError

    def get_cmd_args(self) -> str:
        return "{ks} {cf} {duration} -s {capacity} -k {toppartition} -a {samplers}".format(**self._built_args)
