import time
from invoke import exceptions
from sdcm.tester import ClusterTester
from sdcm.db_stats import PrometheusDBStats
from sdcm.utils.common import skip_optional_stage


class AdmissionControlOverloadTest(ClusterTester):
    """
    Test Scylla load with cassandra-stress to trigger admission control for both read and write
    """
    max_transport_requests = 0

    def check_prometheus_metrics(self, start_time, now):
        prometheus = PrometheusDBStats(self.monitors.nodes[0].external_address)
        node_procs_blocked = 'scylla_transport_requests_blocked_memory'
        node_procs_res = prometheus.query(node_procs_blocked, start_time, now)

        is_admission_control_triggered = False
        for node in node_procs_res:
            if int(node['values'][0][1]) > 0:
                self.log.info('Admission control was triggered')
                is_admission_control_triggered = True

        return is_admission_control_triggered

    def run_load(self, job_num, job_cmd, is_prepare=False):
        is_ever_triggered = False
        if is_prepare and not skip_optional_stage('prepare_write'):
            prepare_stress_queue = self.run_stress_thread(
                stress_cmd=job_cmd,
                duration=self.params.get('prepare_stress_duration'),
                stress_num=job_num,
                prefix='preload-',
                stats_aggregate_cmds=False,
            )
            self.get_stress_results(prepare_stress_queue)
        elif not is_prepare and not skip_optional_stage('main_load'):
            stress_queue = []
            stress_res = []
            stress_queue.append(self.run_stress_thread(stress_cmd=job_cmd, stress_num=job_num,
                                                       stats_aggregate_cmds=False))

            start_time = time.time()
            while stress_queue:
                stress_res.append(stress_queue.pop(0))
                while not all(future.done() for future in stress_res[-1].results_futures):
                    now = time.time()
                    if self.check_prometheus_metrics(start_time=start_time, now=now):
                        is_ever_triggered = True
                        self.kill_stress_thread()
                    start_time = now
                    time.sleep(10)

            if not is_ever_triggered:
                results = []
                for stress in stress_res:
                    try:
                        results.append(self.get_stress_results(stress))
                    except exceptions.CommandTimedOut as ex:
                        self.log.debug('some c-s timed out\n{}'.format(ex))

        return is_ever_triggered

    def run_admission_control(self, prepare_base_cmd, stress_base_cmd, job_num):
        # run a write workload
        if prepare_base_cmd:
            self.run_load(job_num=job_num, job_cmd=prepare_base_cmd, is_prepare=True)
            time.sleep(10)

        # run workload load
        is_admission_control_triggered = self.run_load(job_num=job_num, job_cmd=stress_base_cmd)

        self.verify_no_drops_and_errors(starting_from=self.start_time)
        self.assertTrue(is_admission_control_triggered, 'Admission Control wasn\'t triggered')

    def read_admission_control_load(self):
        """
        Test steps:
        1. Run a write workload as a preparation
        2. Run a read workload
        """
        self.log.debug('Started running read test')
        base_cmd_prepare = self.params.get('prepare_write_cmd')
        base_cmd_r = self.params.get('stress_cmd_r')

        self.run_admission_control(base_cmd_prepare, base_cmd_r, job_num=8)
        self.log.debug('Finished running read test')

    def write_admission_control_load(self):
        """
        Test steps:
        1. Run a write workload without need to preparation
        """
        self.log.debug('Started running write test')
        base_cmd_w = self.params.get('stress_cmd_w')

        self.run_admission_control(None, base_cmd_w, job_num=16)
        self.log.debug('Finished running write test')

    def test_admission_control(self):
        self.write_admission_control_load()

        node = self.db_cluster.nodes[0]
        node.stop_scylla(verify_up=False, verify_down=True)
        node.start_scylla()

        self.read_admission_control_load()
        self.log.info('Admission Control Test has finished with success')
