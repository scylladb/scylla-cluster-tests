import time
from sdcm.tester import ClusterTester


class HintedHandoffTest(ClusterTester):

    def setUp(self):
        super().setUp()
        self.stress_write_cmd = "cassandra-stress write no-warmup cl=ONE n=100123123 " \
                                "-schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=3)' " \
                                "-mode cql3 native -rate 'threads=20 throttle=16000/s' " \
                                "-col 'size=FIXED(1024) n=FIXED(1)' -pop seq=1..100123123 -log interval=15"

        self.stress_read_cmd = "cassandra-stress read no-warmup cl=ONE n=100123123 " \
                               "-schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=3)' " \
                               "-mode cql3 native -rate threads=100 -col 'size=FIXED(1024) n=FIXED(1)'" \
                               " -pop seq=1..100123123 -log interval=15"
        self.start_time = time.time()

    def test_stop_nodes_under_stress(self):
        """
            3 nodes cluster, RF=3.
            While Write n=X with CL=1.
            Stop node1
            Wait some time
            Stop node2
            Wait till write finish
            Start all stopped nodes (1 & 2).
            Wait for hints to be sent.
            Stop node3.
            Read all data n=X with CL=ONE.
        """
        assert len(self.db_cluster.nodes) == 3, "The test requires 3 DB nodes!"
        node1 = self.db_cluster.nodes[0]
        node2 = self.db_cluster.nodes[1]
        node3 = self.db_cluster.nodes[2]
        self.log.info("Running write stress")
        cs_thread_pool = self.run_stress_thread(stress_cmd=self.stress_write_cmd, stress_num=1)
        data_set_size = self.get_data_set_size(self.stress_write_cmd)
        self.wait_data_dir_reaching(data_set_size * 0.5, node=node1)  # 50 percent of data set size (approximation)
        self.log.info("Stopping Scylla on node1")
        node1.stop_scylla()
        self.wait_data_dir_reaching(size=data_set_size, node=node2)  # 75 percent of data set size, not multiplying
        # by 0.75 since it stores data for node1 and for its own, so it will use twice more space
        self.log.info("Stopping Scylla on node2")
        node2.stop_scylla()
        self.log.info("Waiting for stress write to finish.")
        self.verify_stress_thread(cs_thread_pool=cs_thread_pool)
        self.log.info("Starting Scylla on node1 and node2...")
        node1.start_scylla()
        node2.start_scylla()
        self.wait_for_hints_to_be_sent(node=node3, num_dest_nodes=2)
        self.log.info("Stopping Scylla on node3")
        node3.stop_scylla()
        cs_thread_pool = self.run_stress_thread(stress_cmd=self.stress_read_cmd, stress_num=1)
        self.verify_stress_thread(cs_thread_pool=cs_thread_pool)
        self.verify_no_drops_and_errors(starting_from=self.start_time)
