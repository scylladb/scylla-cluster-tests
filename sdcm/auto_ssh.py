import atexit
import logging

from sdcm.remote import LocalCmdRunner

LOGGER = logging.getLogger(__name__)

RSYSLOG_SSH_TUNNEL_LOCAL_PORT = 5000


def start_auto_ssh(docker_name, node, local_port, remote_port):
    """
    Starts a reverse port forwarding with autossh inside a docker container

    :param docker_name: prefix of the docker name (cluster.Setup.test_id() usually would be used)
    :param node: an instance of a class derived from BaseNode that has _ssh_login_info
    :param local_port: the destination port on local machine
    :param remote_port: the source port on the remote
    :return: None
    """

    host_name = node._ssh_login_info['hostname']
    user_name = node._ssh_login_info['user']
    key_path = node._ssh_login_info['key_file']

    local_runner = LocalCmdRunner()
    res = local_runner.run('''
           docker run -d --network=host \
           -e SSH_HOSTNAME={host_name} \
           -e SSH_HOSTUSER={user_name} \
           -e SSH_TUNNEL_HOST=127.0.0.1 \
           -e SSH_TUNNEL_LOCAL={local_port} \
           -e SSH_TUNNEL_REMOTE={remote_port} \
           -e AUTOSSH_GATETIME=0 \
           -v {key_path}:/id_rsa  \
           --restart always \
           --name {docker_name}-{host_name}-autossh jnovack/autossh
       '''.format(**locals()))

    atexit.register(stop_auto_ssh, docker_name, node)
    LOGGER.debug('{docker_name}-{host_name}-autossh {res.stdout}'.format(**locals()))


def stop_auto_ssh(docker_name, node):
    """
    stops an autossh docker instance
    :param docker_name: prefix of the docker name (cluster.Setup.test_id() usually would be used)
    :param node: an instance of a class derived from BaseNode that has _ssh_login_info
    :return: None
    """
    host_name = node._ssh_login_info['hostname']

    LOGGER.debug("killing {docker_name}-{host_name}-autossh".format(**locals()))
    local_runner = LocalCmdRunner()
    local_runner.run("docker rm -f {docker_name}-{host_name}-autossh".format(**locals()), ignore_status=True)
