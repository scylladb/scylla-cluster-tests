import time

from sdcm.tester import ClusterTester
from sdcm.utils.common import skip_optional_stage


class FullClusterStopStart(ClusterTester):

    def test_full_cluster_stop_start(self):
        configure_service_command = r"sudo sed -i '/^\[Service\]/a\RestartSec=40sec' /usr/lib/systemd/system/scylla-server.service"
        daemon_reload_command = "sudo systemctl daemon-reload"
        kill_cmd = "sudo pkill -9 scylla"
        # run cs write
        start_time = time.time()
        if not skip_optional_stage('main_load'):
            stress_queue = self.run_stress_thread(stress_cmd=self.params.get('stress_cmd'))
            self.get_stress_results(queue=stress_queue)
            self.verify_stress_thread(cs_thread_pool=stress_queue)
            # verify read
            stress_read = self.run_stress_thread(stress_cmd=self.params.get('stress_read_cmd'))
            self.verify_stress_thread(cs_thread_pool=stress_read)
        # stop all nodes
        nodes = self.db_cluster.nodes
        for node in nodes:
            node.remoter.run(configure_service_command, ignore_status=True)
            node.remoter.run(daemon_reload_command, ignore_status=True)
            node.remoter.run(kill_cmd, ignore_status=True)
            # wait until node down
            node.wait_db_down(timeout=300)

        # sleep 60 seconds
        time.sleep(60)
        # making sure all nodes are up after RestartSecs are over
        for node in nodes:
            self.log.info("making sure node '{}' is up".format(node))
            node.wait_db_up(verbose=True, timeout=300)

        if not skip_optional_stage('main_load'):
            stress_queue = self.run_stress_thread(stress_cmd=self.params.get('stress_read_cmd'))
            self.verify_stress_thread(cs_thread_pool=stress_queue)
        self.verify_no_drops_and_errors(starting_from=start_time)
