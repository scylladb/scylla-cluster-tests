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
    - The cluster is deployed across multiple availability zones or a simulated rack environment or in multiple regions.

    The validation aims to confirm that no coordination traffic is directed to availability zones where loaders are absent
    """
    validator_name = 'rackaware'

    # This variable defines the allowed percentage variation of system CQL reads directed to availability zones without loaders.
    # These reads, such as those performed by nemeses or non-rack-aware test code, may be routed this way.
    EXPECTED_NON_SYSTEM_READS_PERC = 10

    def validate(self):
        """
        Validates the rack-aware policy by ensuring that no coordination traffic is directed
        to availability zones without loaders.

        This method:
        - Checks if the rack-aware policy is enabled.
        - Retrieves database and loader nodes per region.
        - Validates traffic distribution for each region.
        - Updates the test status based on validation results.

        If validation fails, an error event is published.

        Returns:
            None
        """
        if not self.tester.is_rack_aware_policy:
            LOGGER.info("No workloads were running under the rack-aware policy.")
            return

        validation_passed = set()
        # The case: multi-region cluster
        loaders_per_region = self.tester.loaders.nodes_by_region()
        db_nodes_per_region = self.tester.db_cluster.nodes_by_region()
        for region in self.tester.db_cluster.datacenter:
            (non_rack_db_nodes, rack_db_nodes) = self.map_nodes_per_rack_with_and_without_loader(db_nodes=db_nodes_per_region[region],
                                                                                                 loader_nodes=loaders_per_region[region])

            LOGGER.debug("Nodes in a loader-less AZ: %s; Region: %s", non_rack_db_nodes, region)
            LOGGER.debug("Nodes in a loader AZ: %s; Region: %s", rack_db_nodes, region)
            if not (non_rack_db_nodes or rack_db_nodes):
                ValidatorEvent(message='Rackaware validation. Unable to retrieve node list filtered by loader AZ, or all nodes '
                                       'when loader is absent.',
                               severity=Severity.ERROR).publish()
                validation_passed.add(False)
                continue

            validation_passed.add(self.one_region_validate(
                non_rack_db_nodes=non_rack_db_nodes, rack_db_nodes=rack_db_nodes))

        current_test_status = self.tester.get_test_status
        self.tester.get_test_status = lambda: 'FAILED' if not validation_passed == {True} else current_test_status()

    def map_nodes_per_rack_with_and_without_loader(self, db_nodes, loader_nodes):
        """
        Maps database nodes into two categories:
        - Nodes in availability zones without loaders.
        - Nodes in availability zones with loaders.

        Args:
            db_nodes (list): List of database nodes.
            loader_nodes (list): List of loader nodes.

        Returns:
            tuple: A tuple containing two lists:
                - non_rack_db_nodes: Nodes in loader-less availability zones.
                - rack_db_nodes: Nodes in availability zones with loaders.
        """
        non_rack_db_nodes, rack_db_nodes = [], []
        db_nodes_per_dc_and_rack_id = self.tester.db_cluster.nodes_by_racks_idx_and_regions(db_nodes)
        loader_nodes_per_dc_and_rack_id = self.tester.db_cluster.nodes_by_racks_idx_and_regions(loader_nodes)
        LOGGER.debug("DB nodes per dc and rack: %s", db_nodes_per_dc_and_rack_id)
        LOGGER.debug("Loader nodes per dc and rack: %s", loader_nodes_per_dc_and_rack_id)
        for (region, rack) in db_nodes_per_dc_and_rack_id:
            if (region, rack) not in loader_nodes_per_dc_and_rack_id:
                non_rack_db_nodes.extend(
                    [node.external_address for node in db_nodes_per_dc_and_rack_id[(region, rack)]])
            else:
                rack_db_nodes.extend([node.external_address for node in db_nodes_per_dc_and_rack_id[(region, rack)]])

        return non_rack_db_nodes, rack_db_nodes

    def one_region_validate(self, non_rack_db_nodes, rack_db_nodes):
        """
            Validates rack-aware traffic distribution for a single region.

            This method checks that user-initiated CQL reads are not disproportionately routed
            to database nodes in availability zones (AZs) without loaders. It calculates the
            percentage of reads handled by nodes in loader-less AZs and compares it to the
            expected threshold (`EXPECTED_NON_SYSTEM_READS_PERC`). If the percentage exceeds
            the threshold, an error event is published.

            Args:
                non_rack_db_nodes (list): List of database node IPs in AZs without loaders.
                rack_db_nodes (list): List of database node IPs in AZs with loaders.

            Returns:
                bool: True if validation passes, False otherwise. If no reads are detected,
                      returns None after publishing an error event.
            """
        validation_passed = True
        non_rack_user_cql_reads = 0
        for db_node_ip in non_rack_db_nodes:
            cql_reads = max(0, self.get_cql_reads(db_node_ip=db_node_ip))
            LOGGER.debug("Node %s. Non-system CQl read amounts are being routed to a node in a loader-less AZ: %s",
                         db_node_ip, cql_reads)
            non_rack_user_cql_reads += cql_reads

        rack_user_cql_reads = 0
        for db_node_ip in rack_db_nodes:
            cql_reads = max(0, self.get_cql_reads(db_node_ip=db_node_ip))
            LOGGER.debug("Node %s. Non-system CQl read amounts are being routed to a node in a loader AZ: %s",
                         db_node_ip, cql_reads)
            rack_user_cql_reads += cql_reads

        if not (rack_user_cql_reads or non_rack_user_cql_reads):
            ValidatorEvent(message='Rackaware validation. Reads (non-system CQL) initiated by the user were not received',
                           severity=Severity.ERROR).publish()
            return False

        non_system_cql_reads_perc = 100 * float(non_rack_user_cql_reads) / \
            (float(rack_user_cql_reads) + float(non_rack_user_cql_reads))
        LOGGER.debug("Non-system CQl read amounts are being routed to a node in a loader-less AZ: %s",
                     non_rack_user_cql_reads)
        LOGGER.debug("Non-system CQl read amounts are being routed to a node in a loader AZ: %s", rack_user_cql_reads)
        LOGGER.debug("User-initiated CQL reads as a percentage of all CQL reads: %s", non_system_cql_reads_perc)
        if non_system_cql_reads_perc > self.EXPECTED_NON_SYSTEM_READS_PERC:
            ValidatorEvent(message=f'Rackaware validation. '
                           f'Database nodes in availability zones without loaders received more coordination traffic than the '
                           'expected maximum.'
                           f'\nUser CQL reads are received on Db nodes in AZ with loaders: {rack_user_cql_reads};'
                           f'\nUser CQL reads are received on Db nodes in AZ without loaders: {non_rack_user_cql_reads};\n'
                           f'The percent is {non_system_cql_reads_perc}',
                           severity=Severity.ERROR).publish()
            validation_passed = False

        return validation_passed

    def get_cql_reads(self, db_node_ip: str):
        """
        Retrieves the number of non-system CQL reads for a given database node.

        This method queries Prometheus for the CQL read metrics of the specified node
        and calculates the total number of non-system reads.

        Args:
            db_node_ip (str): The IP address of the database node.

        Returns:
            int: The total number of non-system CQL reads. Returns 0 if no data is available.
        """
        query = 'sum(irate(scylla_cql_reads{instance="%s"} [40s]))-sum(irate(scylla_cql_reads_per_ks{instance="%s"} [40s]))' % \
                (db_node_ip, db_node_ip)
        # Example of result: [{'metric': {}, 'values': [[1741104987.33, '0'], [1741105007.33, '0'], [1741105027.33, '0']]}]
        if results := self.tester.prometheus_db.query(query=query, start=self.tester.start_time, end=time.time()):
            return sum([int(float(value[1])) for value in results[0]["values"]]) if results[0]["values"] else 0
        return 0
