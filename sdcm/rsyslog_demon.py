import atexit
import os.path

from sdcm.remote import LocalCmdRunner


rsys_docker_id = None


# TODO: call start_rsyslog from tester, and pass the port to each node


def start_rsyslog(name, log_dir, port=None):
    global rsys_docker_id

    atexit.register(stop_rsyslog)
    log_dir = os.path.abspath(log_dir)
    port = '514:{}'.format(port) if port else '514'

    # TODO: template the rsyslog.conf with the correct user

    local_runner = LocalCmdRunner()
    res = local_runner.run('''
        mkdir -p {log_dir};
        docker run -d \
        -v ${{HOME}}:${{HOME}} \
         -v `pwd`/etc/rsyslog.conf:/etc/rsyslog.conf \
          -p {port} -it -v {log_dir}:/logs --name {name} rsyslog/syslog_appliance_alpine

    '''.format(name=name, port=port, log_dir=log_dir))

    rsys_docker_id = res.stdout.strip()
    res = local_runner.run('''
           docker port {0} 514
           '''.format(rsys_docker_id))

    print res.stdout.strip()

    return res.stdout.strip()


def stop_rsyslog():
    if rsys_docker_id:
        local_runner = LocalCmdRunner()
        local_runner.run('''
                docker kill {id}
            '''.format(id=rsys_docker_id), ignore_status=True)

        local_runner.run('''
                   docker rm {id}
               '''.format(id=rsys_docker_id), ignore_status=True)


if __name__ == "__main__":
    start_rsyslog('fruch', './logs', 514)

    import time
    time.sleep(3000)
