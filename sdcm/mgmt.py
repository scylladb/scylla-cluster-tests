import requests
import logging
import json
import time
import datetime

from sdcm import wait

logger = logging.getLogger(__name__)

STATUS_DONE = 'done'
STATUS_ERROR = 'error'
MANAGER_IDENTITY_FILE = '/tmp/scylla_manager_pem'

from enum import Enum

class TaskStatus(Enum):
   NEW = "NEW"
   RUNNING = "RUNNING"
   DONE = "DONE"
   UNKNOWN = "UNKNOWN"
   ERROR = "ERROR"
   STOPPED = "STOPPED"

   @staticmethod
   def from_str(output_str):
        output_str = output_str.upper()
        if output_str in ('NEW'):
            return TaskStatus.NEW
        elif output_str in ('RUNNING'):
            return TaskStatus.RUNNING
        elif output_str in ('DONE'):
            return TaskStatus.DONE
        elif output_str in ('ERROR'):
            return TaskStatus.ERROR
        elif output_str in ('UNKNOWN'):
            return TaskStatus.UNKNOWN
        elif output_str in ('STOPPED'):
            return TaskStatus.STOPPED
        else:
            raise ScyllaManagerError("Could not recognize returned task status: {}".format(output_str))

class ScyllaManagerError(Exception):
    """
    A custom exception for Manager related errors
    """
    pass

class ScyllaLogicObj(object):

    def __init__(self, id):
        self.id = id


class ManagerTask(ScyllaLogicObj):

    def __init__(self, task_id, mgr_cluster):
        ScyllaLogicObj.__init__(self, id=task_id)
        self.mgr_cluster = mgr_cluster
        self.mgr_tool = mgr_cluster.mgr_tool

    def stop(self):
        cmd = "task stop {} -c {}".format(self.id, self.mgr_cluster.id)
        res = self.mgr_tool.run_sctool_cmd(cmd=cmd, is_verify_errorless_result=True)
        assert self.status != TaskStatus.RUNNING

    def start(self):
        cmd = "task start --continue {} -c {}".format(self.id, self.mgr_cluster.id)
        res = self.mgr_tool.run_sctool_cmd(cmd=cmd, is_verify_errorless_result=True)
        assert self.status not in [TaskStatus.STOPPED]

    @property
    def status(self):
        """
        Gets the repair task's status
        """
        cmd = "task list -c {}".format(self.mgr_cluster.id)
        res = self.mgr_tool.run_sctool_cmd(cmd=cmd, is_verify_errorless_result=True)
        if self.id not in res.stdout:
            logger.error("Encountered an error on '{}' command response: {}".format(cmd, str(res.stdout)))
            raise ScyllaManagerError("Encountered an error on '{}' command response".format(cmd))
        lines = res.stdout.split('\n')
        status = TaskStatus.UNKNOWN
        for line in lines:
            if self.id in line:
                raw_status = line.split()[-2]
                status = TaskStatus.from_str(raw_status)
                break
        logger.debug("Task: {} status is: {}".format(self.id,str(status)))
        return status

    @property
    def progress(self):
        """
        Gets the repair task's progress
        """
        if self.status == TaskStatus.NEW:
            return "0%"
        cmd = "task progress {} -c {}".format(self.id, self.mgr_cluster.id)
        res = self.mgr_tool.run_sctool_cmd(cmd=cmd)
        if not res or "Progress" not in res.stdout:
            logger.error("Encountered an error on '{}' command response: {}".format(cmd, str(res)))
            raise ScyllaManagerError("Encountered an error on '{}' command response".format(cmd))
        lines = res.stdout.split('\n')
        progress = 'N/A'
        for line in lines:
            if "Progress" in line:
                progress = line.split()[3]
                break
        logger.debug("Task: {} progress is: {}".format(self.id,progress))
        return progress

    def is_status_in_list(self, list_status, check_task_progress=False):
        """
        Check if the status of a given task is in list
        :param list_status:
        :return:
        """
        status = self.status
        if check_task_progress and status != TaskStatus.NEW: # check progress for all statuses except 'NEW'
            progress = self.progress
        return self.status in list_status

    def wait_for_status(self, list_status, check_task_progress=True):
        text = "Waiting until task: {} reaches status of: {}".format(self.id, list_status)
        is_status_reached = False
        is_status_reached = wait.wait_for(func=self.is_status_in_list, step=60,
                                          text=text, list_status=list_status, check_task_progress=check_task_progress, timeout=3600)
        return is_status_reached

    def wait_for_status_done(self):
        """
        Wait and report that task is eventually reaching a 'final' status. meaning one of: done/error/stopped
        :return:
        """
        logger.debug("Waiting for task: {} to be done..".format(self.id))
        cur_status = self.status
        if cur_status == TaskStatus.DONE:
            return True
        if cur_status in [TaskStatus.ERROR, TaskStatus.STOPPED]:
            return False

        if cur_status == TaskStatus.NEW:
            logger.debug("Waiting for task: {} to start..".format(self.id))
            list_status = [TaskStatus.RUNNING, TaskStatus.ERROR, TaskStatus.DONE]
            res = self.wait_for_status(list_status=list_status)
            if not res:
                raise ScyllaManagerError("Unexpected result on waiting for task {} status".format(self.id))
            cur_status = self.status
            if cur_status == TaskStatus.ERROR:
                return False

        if cur_status == TaskStatus.RUNNING:
            logger.debug("Waiting for task: {} to finish running..".format(self.id))
            list_status = [TaskStatus.DONE, TaskStatus.ERROR]
            res = self.wait_for_status(list_status=list_status)
            if not res:
                raise ScyllaManagerError("Unexpected result on waiting for task {} status {}".format(self.id, list_status))
            cur_status = self.status
            if cur_status == TaskStatus.ERROR:
                return False
        final_state = self.status
        logger.debug("Task: {} final state is: {}".format(self.id, str(final_state)))
        return self.status == TaskStatus.DONE

class ManagerCluster(ScyllaLogicObj):

    def __init__(self, mgr_tool, cluster_id):
        if not mgr_tool:
            raise ScyllaManagerError("Cannot create a Manager Cluster where no 'manager' host parameter is given")
        ScyllaLogicObj.__init__(self, id=cluster_id)
        self.mgr_tool = mgr_tool

    def create_repair_task(self):
        cmd = "repair -c {}".format(self.id)
        res = self.mgr_tool.run_sctool_cmd(cmd=cmd)
        if not res:
            raise ScyllaManagerError("Unknown failure for sctool {} command".format(cmd))

        if "no matching units found" in res.stderr:
            raise ScyllaManagerError("Manager cannot run repair where no keyspace exists.")

        if 'repair' not in res.stdout:
            logger.error("Encountered an error on '{}' command response".format(cmd))
            raise ScyllaManagerError(res.stderr)

        task_id = res.stdout.split('\n')[0]
        logger.debug("Created task id is: {}".format(task_id))

        return ManagerTask(task_id=task_id, mgr_cluster=self) # return the manager's new repair-task-id

def verify_errorless_result(cmd, res):
    if not res or res.stderr:
        logger.error("Encountered an error on '{}' command response: {}".format(cmd, str(res)))
        raise ScyllaManagerError("Encountered an error on '{}' command response".format(cmd))

class ScyllaManagerTool(object):
    """
    Provides communication with scylla-manager, operating sctool commands and ssh-scripts.
    """

    def __init__(self, manager_node):
        self.manager_node = manager_node

    @property
    def version(self):
        cmd = "version"
        res = self.run_sctool_cmd(cmd=cmd, is_verify_errorless_result=True)
        logger.info("Manager version is: {}".format(res.stdout))
        return res.stdout

    def _mgr_remoter_run(self, cmd):
        return self.manager_node.remoter.run(cmd)

    def run_sctool_cmd(self, cmd, is_verify_errorless_result = False):
        logger.debug("Issuing: 'sctool {}'".format(cmd))
        res = self._mgr_remoter_run(cmd='sudo sctool {}'.format(cmd))
        if is_verify_errorless_result:
            verify_errorless_result(cmd=cmd, res=res)
        return res

    def get_cluster(self, cluster_name):
        cmd = 'cluster list'
        res = self.run_sctool_cmd(cmd=cmd, is_verify_errorless_result=True)

        if not res.stdout or cluster_name not in res.stdout:
            logger.debug('Cluster {} not found in scylla-manager'.format(cluster_name))
            return None

        cluster_id = "N/A"
        lines = res.stdout.split('\n')
        for line in lines:
            if cluster_name in line:
                cluster_id = line.split()[1]
                break
        logger.debug("Cluster: {} ID is: {}".format(cluster_name, cluster_id))
        return ManagerCluster(mgr_tool=self,cluster_id=cluster_id)


    def scylla_mgr_ssh_setup(self, node_ip, user='centos', identity_file='/tmp/scylla-test', manager_user='scylla-manager', manager_identity_file=MANAGER_IDENTITY_FILE):
        """
        scyllamgr_ssh_setup -u <username> -i <path to private key> --m <manager username> -o <path to manager private key> [HOST...]
          -u --user				SSH user name used to connect to hosts
          -i --identity-file			path to identity file containing SSH private key
          -m --manager-user			user name that will be created and configured on hosts, default scylla-manager
          -o --manager-identity-file		path to identity file containing SSH private key for MANAGER_USERNAME, if there is no such file it will be created
          -d --discover				use first host to discover and setup all hosts in a cluster


        :param node_ip:
        :param identity_file:
        :param manager_user:
        :return:

        sudo scyllamgr_ssh_setup --user centos --identity-file /tmp/scylla-qa-ec2 --manager-user scylla-manager --manager-identity-file /tmp/scylla_manager_pem --discover 54.158.51.22"
        """
        cmd = 'sudo scyllamgr_ssh_setup --user {} --identity-file {} --manager-user {} --manager-identity-file {} --discover {}'.format(user, identity_file, manager_user, manager_identity_file, node_ip)
        logger.debug("SSH setup command is: {}".format(cmd))
        res = self._mgr_remoter_run(cmd=cmd)
        verify_errorless_result(cmd=cmd, res=res)

    def add_cluster(self, cluster_name, host):
        """
        Add cluster to management
        :param cluster_name: cluster name
        :param host: cluster node IP-s
        :return: cluster id

        --host string              hostname or IP of one of the cluster nodes
        -n, --name alias               alias you can give to your cluster
        --ssh-identity-file path   path to identity file containing SSH private key
        --ssh-user name            SSH user name used to connect to the cluster nodes

        """
        logger.debug("Configuring ssh setup for cluster using {} node before adding the cluster: {}".format(host, cluster_name))
        self.scylla_mgr_ssh_setup(node_ip=host)
        identity_file_centos = '/tmp/scylla-test'
        ssh_user='scylla-manager'
        manager_identity_file=MANAGER_IDENTITY_FILE
        sctool_cmd = 'cluster add --host={} --ssh-identity-file={} --ssh-user={} --name={}'.format(host, manager_identity_file, ssh_user, cluster_name)
        logger.debug("Cluster add command is: {}".format(sctool_cmd))
        res = self.run_sctool_cmd(sctool_cmd)
        if not res or 'Cluster added' not in res.stderr:
            logger.error("Encountered an error on 'sctool cluster add' command response")
            logger.error(res)
            raise ScyllaManagerError("Encountered an error on 'sctool cluster add' command response")
        cluster_id = res.stdout.split('\n')[0] # return MgrCluster instance with the manager's new cluster-id
        return ManagerCluster(mgr_tool=self, cluster_id=cluster_id)


class ScyllaMgmt(object):
    """
    Provides communication with scylla-manager via REST API
    """

    def __init__(self, server, port=9090):
        self._url = 'http://{}:{}/api/v1/'.format(server, port)

    def get(self, path, params={}):
        resp = requests.get(url=self._url + path, params=params)
        if resp.status_code not in [200, 201, 202]:
            err_msg = 'GET request to scylla-manager failed! error: {}'.format(resp.content)
            logger.error(err_msg)
            raise Exception(err_msg)
        try:
            return json.loads(resp.content)
        except Exception as ex:
            logger.error('Failed load data from json %s, error: %s', resp.content, ex)
        return resp.content

    def post(self, path, data):
        resp = requests.post(url=self._url + path, data=json.dumps(data))
        if resp.status_code not in [200, 201]:
            err_msg = 'POST request to scylla-manager failed! error: {}'.format(resp.content)
            logger.error(err_msg)
            raise Exception(err_msg)
        return resp

    def put(self, path, data={}):
        resp = requests.put(url=self._url + path, data=json.dumps(data))
        if resp.status_code not in [200, 201]:
            err_msg = 'PUT request to scylla-manager failed! error: {}'.format(resp.content)
            logger.error(err_msg)
            raise Exception(err_msg)
        return resp

    def delete(self, path):
        resp = requests.delete(url=self._url + path)
        if resp.status_code not in [200, 204]:
            err_msg = 'DELETE request to scylla-manager failed! error: {}'.format(resp.content)
            logger.error(err_msg)
            raise Exception(err_msg)

    def get_cluster(self, cluster_name):
        """
        Get cluster by name
        :param cluster_name: cluster name
        :return: cluster id if found, otherwise None
        """
        resp = self.get('clusters', params={'name': cluster_name})

        if not resp:
            logger.debug('Cluster %s not found in scylla-manager', cluster_name)
            return None
        return resp[0]['id']

    def add_cluster(self, cluster_name, hosts, shard_count=16):
        """
        Add cluster to management
        :param cluster_name: cluster name
        :param hosts: list of cluster node IP-s
        :param shard_count: number of shards in nodes
        :return: cluster id
        """
        cluster_obj = {'name': cluster_name, 'hosts': hosts, 'shard_count': shard_count}
        resp = self.post('clusters', cluster_obj)
        return resp.headers['Location'].split('/')[-1]

    def delete_cluster(self, cluster_id):
        """
        Remove cluster from management
        :param cluster_id: cluster id/name
        :return: nothing
        """
        self.delete('cluster/{}'.format(cluster_id))

    def get_schedule_task(self, cluster_id):
        """
        Find auto scheduling repair task created automatically on cluster creation
        :param cluster_id: cluster id
        :return: task dict
        """
        resp = []
        while not resp:
            resp = self.get(path='cluster/{}/tasks'.format(cluster_id), params={'type': 'repair_auto_schedule'})
        return resp[0]

    def disable_task_schedule(self, cluster_id, task):
        start_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=30)
        task['schedule'] = {'start_date': start_time.isoformat() + 'Z',
                            'interval_days': 0,
                            'num_retries': 0}
        task['enabled'] = True

        self.put(path='cluster/{}/task/repair_auto_schedule/{}'.format(cluster_id, task['id']), data=task)

    def start_repair_task(self, cluster_id, task_id, task_type='repair'):
        self.put(path='cluster/{}/task/{}/{}/start'.format(cluster_id, task_type, task_id))

    def get_repair_tasks(self, cluster_id):
        """
        Get cluster repair tasks(repair units per cluster keyspace)
        :param cluster_id: cluster id
        :return: repair tasks dict with unit id as key, task id as value
        """
        resp = []
        while not resp:
            resp = self.get(path='cluster/{}/tasks'.format(cluster_id), params={'type': 'repair'})
        tasks = {}
        for task in resp:
            unit_id = task['properties']['unit_id']
            if unit_id not in tasks and 'status' not in task:
                tasks[unit_id] = task['id']
        return tasks

    def get_task_progress(self, cluster_id, repair_unit):
        try:
            return self.get(path='cluster/{}/repair/unit/{}/progress'.format(cluster_id, repair_unit))
        except Exception as ex:
            logger.exception('Failed to get repair progress: %s', ex)
        return None

    def run_repair(self, cluster_id, timeout=0):
        """
        Run repair for cluster
        :param cluster_id: cluster id
        :param timeout: timeout in seconds to wait for repair done
        :return: repair status(True/False)
        """
        sched_task = self.get_schedule_task(cluster_id)

        if sched_task['schedule']['interval_days'] > 0:
            self.disable_task_schedule(cluster_id, sched_task)

        self.start_repair_task(cluster_id, sched_task['id'], 'repair_auto_schedule')

        tasks = self.get_repair_tasks(cluster_id)

        status = True
        # start repair tasks one by one:
        # currently scylla-manager cannot execute tasks simultaneously, only one can run
        logger.info('Start repair tasks per cluster %s keyspace', cluster_id)
        unit_to = timeout / len(tasks)
        start_time = time.time()
        for unit, task in tasks.iteritems():
            self.start_repair_task(cluster_id, task, 'repair')
            task_status = self.wait_for_repair_done(cluster_id, unit, unit_to)
            if task_status['status'] != STATUS_DONE or task_status['error']:
                logger.error('Repair unit %s failed, status: %s, error count: %s', unit,
                             task_status['status'], task_status['error'])
                status = False

        logger.debug('Repair finished with status: %s, time elapsed: %s', status, time.time() - start_time)
        return status

    def wait_for_repair_done(self, cluster_id, unit, timeout=0):
        """
        Wait for repair unit task finished
        :param cluster_id: cluster id
        :param unit: repair unit id
        :param timeout: timeout in seconds to wait for repair unit done
        :return: task status dict
        """
        done = False
        status = {'status': 'unknown', 'percent': 0, 'error': 0}
        interval = 10
        wait_time = 0

        logger.info('Wait for repair unit done: %s', unit)
        while not done and wait_time <= timeout:
            time.sleep(interval)
            resp = self.get_task_progress(cluster_id, unit)
            if not resp:
                break
            status['status'] = resp['status']
            status['percent'] = resp['percent_complete']
            if resp['error']:
                status['error'] = resp['error']
            if status['status'] in [STATUS_DONE, STATUS_ERROR]:
                done = True
            logger.debug('Repair status: %s', status)
            if timeout:
                wait_time += interval
                if wait_time > timeout:
                    logger.error('Waiting for repair unit %s: timeout expired: %s', unit, timeout)
        return status
