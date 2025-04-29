import time

from sdcm.cluster import BaseNode
from sdcm.tester import ClusterTester


class ScaleUpTest(ClusterTester):

    ingest_time = 0
    rebuild_duration = 0

    def test_write_and_rebuild_time(self):
        """
        Test steps:

        1. Measure time of inserting data to the cluster
        2. Delete data on one node and measure time of rebuild
        3. Publish results
        """
        # run a write workload
        base_cmd_w = self.params.get('stress_cmd_w')
        self.run_fstrim_on_all_db_nodes()
        stress_queue = []
        if not isinstance(base_cmd_w, list) and len(base_cmd_w) != len(self.loaders.nodes):
            raise AssertionError("stress_cmd_w should be a list of commands and be equal to number of loaders")
        for stress_cmd in base_cmd_w:
            stress_queue.append(self.run_stress_thread(
                stress_cmd=stress_cmd, stats_aggregate_cmds=False, round_robin=True))
        results = []
        for stress in stress_queue:
            results.append(self.get_stress_results(queue=stress, store_results=False)[0])
        result = max(results, key=lambda x: x['total operation time'])["total operation time"]
        self.ingest_time = result
        self.log.info("Write workload finished with time: %s", result)
        self.wait_no_compactions_running(n=90, sleep_time=60)
        node = self.db_cluster.nodes[0]
        self.destroy_node_data(node)
        start_time = time.time()
        node.run_nodetool("rebuild")
        rebuild_duration = time.time() - start_time
        self.rebuild_duration = rebuild_duration
        self.log.info("Rebuild finished with time: %s", rebuild_duration)

    def destroy_node_data(self, node: BaseNode):
        """
        Destroy data on node
        """
        self.log.info("Destroy all data for keyspace1 on node %s", node)
        node.stop_scylla_server()
        node.remoter.sudo("find /var/lib/scylla/data/keyspace1/standard1-* -type f | sudo xargs rm -f")
        node.start_scylla_server()
        self.log.info("Data destroyed on node %s", node)

    def get_email_data(self):
        self.log.info("Prepare data for email")
        email_data = {}

        try:
            email_data = self._get_common_email_data()
        except Exception as error:
            self.log.exception("Error in gathering common email data: Error:\n%s", error, exc_info=error)

        email_data.update({
            "ingest_time": self.ingest_time,
            "rebuild_duration": self.rebuild_duration,
            "subject": f"Scale up test results - {email_data['scylla_instance_type']}",
        })
        return email_data
