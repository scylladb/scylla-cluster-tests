import logging
import time

from sdcm.sct_events import Severity
from sdcm.sct_events.teardown_validators import ValidatorEvent
from sdcm.teardown_validators.base import TeardownValidator

LOGGER = logging.getLogger(__name__)


class RackawareValidator(TeardownValidator):  # pylint: disable=too-few-public-methods
    """
    Rack-aware validation is enabled under these conditions:
    - A rack-aware policy is configured.
    - A single loader is employed (due to Scylla rack metric limitation).
    - The cluster is deployed across multiple availability zones or a simulated rack environment.

    The validation aims to confirm that no coordination traffic is directed to availability zones where loaders are absent
    """
    validator_name = 'rackaware'

    # This variable defines the allowed percentage variation of system CQL reads directed to availability zones without loaders.
    # These reads, such as those performed by nemeses or non-rack-aware test code, may be routed this way.
    EXPECTED_NON_SYSTEM_READS_PERC = 10

    @property
    def count_loaders(self):
        loaders = sum(len(loaders.nodes) for loaders in self.tester.loaders_multitenant)
        LOGGER.debug("Loaders count: %s", loaders)
        return loaders

    def validate(self):
        if not self.tester.is_rack_aware_policy:
            LOGGER.info("No workloads were running under the rack-aware policy.")
            return

        if not (self.count_loaders == 1 and self.tester.db_cluster.racks_count > 1):
            LOGGER.info("This test environment is not configured for rack-aware policy validation, "
                        "as it lacks a multi-AZ or simulated rack DB cluster and a single loader.")
            return

        loader = self.tester.loaders.nodes[0]
        non_rack_db_nodes = self.get_db_non_rack_coordless_nodes(dc=loader.datacenter,
                                                                 rack=loader.node_rack)
        validation_passed = True
        for db_node_ip in non_rack_db_nodes:
            LOGGER.info("Get CQL reads for user keyspace on instance: %s", db_node_ip)

            # The metrics `reads` and `reads_per_ks` provide different read counts.
            # - `reads` tallies all read operations.
            # - `reads_per_ks` is limited to reads within the system keyspace (see
            # https://github.com/scylladb/scylladb/commit/1cfa4584091f58187225b9db7a0186aabdc93a8f).
            #
            # Consequently, (reads - reads_per_ks) gives the read count for user keyspaces.
            query = 'sum(irate(scylla_cql_reads{instance="%s"} [40s]))-sum(irate(scylla_cql_reads_per_ks{instance="%s"} [40s]))' % \
                    (db_node_ip, db_node_ip)
            if not (non_system_cql_reads := self.get_cql_reads(query=query)):
                ValidatorEvent(
                    message=f'Instance {db_node_ip}: empty Prometheus data. Query: {query}', severity=Severity.WARNING).publish()
                continue

            query = 'sum(irate(scylla_cql_reads{instance="%s"} [40s]))' % (db_node_ip)
            if not (all_cql_reads := self.get_cql_reads(query=query)):
                ValidatorEvent(
                    message=f'Instance {db_node_ip}: empty Prometheus data. Query: {query}', severity=Severity.WARNING).publish()
                continue

            if all_cql_reads == 0:
                ValidatorEvent(message=f'Instance {db_node_ip} did not receive cql reads',
                               severity=Severity.WARNING).publish()
            else:
                non_system_cql_reads_perc = 100 * float(non_system_cql_reads) / float(all_cql_reads)
                LOGGER.debug("Node %s: non system CQl reads %s", db_node_ip, non_system_cql_reads)
                LOGGER.debug("Node %s: all CQl reads %s", db_node_ip, all_cql_reads)
                LOGGER.debug("Node %s: non system CQl reads percent is %s", db_node_ip, non_system_cql_reads_perc)
                if non_system_cql_reads_perc > self.EXPECTED_NON_SYSTEM_READS_PERC:
                    ValidatorEvent(message=f'Instance {db_node_ip} is receiving unintended coordination traffic, despite being located '
                                           f'in availability zones without loaders.', severity=Severity.ERROR).publish()
                    validation_passed = False

        self.tester.get_test_status = lambda: 'FAILED' if not validation_passed else 'SUCCESS'

    def get_cql_reads(self, query, ):
        # Example of result: [{'metric': {}, 'values': [[1741104987.33, '0'], [1741105007.33, '0'], [1741105027.33, '0']]}]
        if results := self.tester.prometheus_db.query(query=query, start=self.tester.start_time, end=time.time()):
            return sum([int(float(value[1])) for value in results[0]["values"]]) if results[0]["values"] else 0
        return None

    def get_db_non_rack_coordless_nodes(self, dc: str, rack: str):
        """
        Retrieve database nodes in availability zones without loaders.
        """
        non_rack_nodes = [
            db_node.external_address for db_node in self.tester.db_cluster.nodes if db_node.datacenter != dc and db_node.node_rack != rack]
        LOGGER.debug("Db nodes are in availability zones without loaders: %s", non_rack_nodes)
        return non_rack_nodes
