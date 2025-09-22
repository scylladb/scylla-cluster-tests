# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2020 ScyllaDB

import os
import logging
import getpass
from functools import cached_property
from tempfile import mkstemp
from typing import Optional

from sdcm.utils.docker_utils import ContainerManager

RSYSLOG_IMAGE = "rsyslog/syslog_appliance_alpine:latest"
RSYSLOG_PORT = 514


LOGGER = logging.getLogger(__name__)


class RSyslogContainerMixin:  # pylint: disable=too-few-public-methods
    @cached_property
    def rsyslog_confpath(self) -> str:
        return generate_rsyslog_conf_file()

    @property
    def rsyslog_port(self) -> Optional[int]:
        return ContainerManager.get_container_port(self, "rsyslog", RSYSLOG_PORT)

    @property
    def rsyslog_log_dir(self) -> str:
        # This should work after process that had run container was terminated and another one starts
        # that is why it is not using variable to store log directory instead
        return ContainerManager.get_host_volume_path(
            instance=self,
            container_name="rsyslog",
            path_in_container="/logs",
        )

    def rsyslog_container_run_args(self, logdir: str) -> dict:
        basedir, logdir = os.path.split(logdir)
        logdir = os.path.abspath(os.path.join(os.environ.get("_SCT_LOGDIR", basedir), logdir))
        os.makedirs(logdir, exist_ok=True)
        LOGGER.info("rsyslog will store logs at %s", logdir)

        volumes = {
            "/etc/passwd": {"bind": "/etc/passwd", "mode": "ro"},
            "/etc/group": {"bind": "/etc/group", "mode": "ro"},
            self.rsyslog_confpath: {"bind": "/etc/rsyslog.conf", "mode": "ro"},
            logdir: {"bind": "/logs"},
        }

        return dict(
            image=RSYSLOG_IMAGE,
            name=f"{self.name}-rsyslog",
            auto_remove=True,
            ports={
                f"{RSYSLOG_PORT}/tcp": None,
            },
            volumes=volumes,
        )


def generate_rsyslog_conf_file():
    conf_fd, conf_path = mkstemp(prefix="sct-rsyslog", suffix=".conf")
    with os.fdopen(conf_fd, "w") as file_obj:
        file_obj.write(RSYSLOG_CONF.format(owner=getpass.getuser(), port=RSYSLOG_PORT))
    LOGGER.debug("rsyslog conf file created in '%s'", conf_path)
    return conf_path


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

input(type="imptcp" port="{port}")
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
