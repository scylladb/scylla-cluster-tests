import os
import os.path
import logging
import getpass
import atexit

from tempfile import mkstemp

from sdcm.remote import LocalCmdRunner

LOGGER = logging.getLogger(__name__)


RSYSLOG_DOCKER_ID = None
RSYSLOG_CONF_PATH = None

RSYSLOG_CONF = """
global(processInternalMessages="on")

module(load="builtin:omfile" fileOwner="{owner}" dirOwner="{owner}")
#module(load="imtcp" StreamDriver.AuthMode="anon" StreamDriver.Mode="1")
module(load="impstats") # config.enabled=`echo $ENABLE_STATISTICS`)
module(load="imrelp")
module(load="imptcp")
module(load="imudp" TimeRequery="500")
module(load="omstdout")
module(load="mmjsonparse")
module(load="mmutf8fix")

input(type="imptcp" port="514")
# input(type="imudp" port="514")
# input(type="imrelp" port="1601")

template(name="FileFormat" type="string"
         string= "%timestamp:::date-year%-%timestamp:::date-month%-%timestamp:::date-day%T%timestamp:::date-hour%:%timestamp:::date-minute%:%timestamp:::date-second%%timestamp:::date-tzoffsdirection%%timestamp:::date-tzoffshour%:%timestamp:::date-tzoffsmin%  %hostname% !%syslogseverity-text::7:fixed-width,uppercase% | %syslogtag%%msg:::sp-if-no-1st-sp%%msg:::drop-last-lf%\n"
        )

template(name="log_to_files_dynafile" type="string" string="/logs/hosts/%hostname:::secpath-replace%/messages.log")
ruleset(name="log_to_files") {{
        action(type="omfile" dynafile="log_to_files_dynafile" name="log_to_logfiles" template="FileFormat")
}}


# includes done explicitely
# include(file="/etc/rsyslog.conf.d/log_to_logsene.conf" config.enabled=`echo $ENABLE_LOGSENE`)
# include(file="/etc/rsyslog.conf.d/log_to_files.conf" config.enabled=`echo $ENABLE_LOGFILES`)

#################### default ruleset begins ####################

# we emit our own messages to docker console:
syslog.* :omstdout:

include(file="/config/droprules.conf" mode="optional")  # this permits the user to easily drop unwanted messages

action(name="main_utf8fix" type="mmutf8fix" replacementChar="?")

include(text=`echo $CNF_CALL_LOG_TO_LOGFILES`)
include(text=`echo $CNF_CALL_LOG_TO_LOGSENE`)

"""


def generate_conf_file():
    fd, conf_path = mkstemp(prefix="sct-rsyslog", suffix=".conf")
    with os.fdopen(fd, 'w') as file_obj:
        file_obj.write(RSYSLOG_CONF.format(owner=getpass.getuser()))
    LOGGER.debug("rsyslog conf file created in '%s'", conf_path)
    return conf_path


def start_rsyslog(docker_name, log_dir, port="514"):
    """
    Start rsyslog in a docker, for getting logs from db-nodes

    :param docker_name: name of the docker instance
    :param log_dir: directory where to store the logs
    :param port: [Optional] the port binding for the docker run

    :return: the listening port
    """
    global RSYSLOG_DOCKER_ID, RSYSLOG_CONF_PATH

    log_dir = os.path.abspath(log_dir)

    # cause of docker-in-docker, we need to capture the host log dir for mounting it
    # _SCT_BASE_DIR is set in hydra.sh
    base_dir = os.environ.get("_SCT_BASE_DIR", None)
    if base_dir:
        mount_log_dir = os.path.join(base_dir, os.path.basename(log_dir))
    else:
        mount_log_dir = log_dir

    conf_path = generate_conf_file()
    RSYSLOG_CONF_PATH = conf_path
    local_runner = LocalCmdRunner()
    res = local_runner.run('''
        mkdir -p {log_dir};
        docker run --rm -d \
        -v /etc/passwd:/etc/passwd:ro \
        -v /etc/group:/etc/group:ro \
        -v {mount_log_dir}:/logs \
        -v {conf_path}:/etc/rsyslog.conf \
        -p {port} \
        --name {docker_name}-rsyslogd rsyslog/syslog_appliance_alpine
    '''.format(**locals()))

    RSYSLOG_DOCKER_ID = res.stdout.strip()
    LOGGER.info("Rsyslog started. Container id: %s", RSYSLOG_DOCKER_ID)

    atexit.register(stop_rsyslog)

    res = local_runner.run('docker port {0} 514'.format(RSYSLOG_DOCKER_ID))
    listening_port = res.stdout.strip().split(':')[1]

    return listening_port


def stop_rsyslog():
    global RSYSLOG_DOCKER_ID, RSYSLOG_CONF_PATH
    if RSYSLOG_DOCKER_ID:
        local_runner = LocalCmdRunner()
        local_runner.run("docker kill {id}".format(id=RSYSLOG_DOCKER_ID), ignore_status=True)

    if RSYSLOG_CONF_PATH:
        try:
            os.remove(RSYSLOG_CONF_PATH)
        except Exception:
            pass

    RSYSLOG_CONF_PATH = None
    RSYSLOG_DOCKER_ID = None
