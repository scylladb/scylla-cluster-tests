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
# Copyright (c) 2021 ScyllaDB

from textwrap import dedent


# pylint: disable=anomalous-backslash-in-string


def configure_rsyslog_rate_limits_script(interval: int, burst: int) -> str:
    # Configure rsyslog.  Use obsolete legacy format here because it's easier to redefine imjournal parameters.
    return dedent(fr"""
    grep imjournalRatelimitInterval /etc/rsyslog.conf | cat <<EOF >> /etc/rsyslog.conf
    #
    # The following configuration was added by SCT.
    #
    \$ModLoad imjournal
    \$imjournalRatelimitInterval {interval}
    \$imjournalRatelimitBurst {burst}
    EOF
    """)


def configure_rsyslog_target_script(host: str, port: int) -> str:
    return f'echo "action(type=\\"omfwd\\" Target=\\"{host}\\" Port=\\"{port}\\" Protocol=\\"tcp\\")" >> /etc/rsyslog.conf\n'


def configure_syslogng_target_script(host: str, port: int, throttle_per_second: int, hostname: str = "") -> str:
    return dedent("""
        source_name=`cat /etc/syslog-ng/syslog-ng.conf | tr -d "\\n" | tr -d "\\r" | sed -r "s/\\}};/\\}};\\n/g; \
        s/source /\\nsource /g" | grep -P "^source.*system\\(\\)" | cut -d" " -f2`
        disk_buffer_option=""
        if syslog-ng -V | grep disk; then
            disk_buffer_option="disk-buffer(
                    mem-buf-size(100000)
                    disk-buf-size(2000000)
                    reliable(yes)
                    dir(\\\"/tmp\\\")
                )"
        fi

        if grep -P "keep-timestamp\\([^)]+\\)" /etc/syslog-ng/syslog-ng.conf; then
            sed -i -r "s/keep-timestamp([ ]*no[ ]*)/keep-timestamp(yes)/g" /etc/syslog-ng/syslog-ng.conf
        else
            sed -i -r "s/([ \t]*options[ \t]*\\\\{{)/\\\\1\\n  keep-timestamp(yes);\\n/g" /etc/syslog-ng/syslog-ng.conf
        fi

        grep "destination remote_sct" /etc/syslog-ng/syslog-ng.conf || cat <<EOF >>/etc/syslog-ng/syslog-ng.conf
        destination remote_sct {{
            network(
                "{host}"
                transport("tcp")
                port({port})
                throttle({throttle_per_second})
                $disk_buffer_option
            );
        }};

        destination d_system {{ file("/var/log/system.log"); }};
        log {{ source($source_name); destination(d_system); }};

        EOF

        if ! grep -P "log {{.*destination\\\\(remote_sct\\\\)" /etc/syslog-ng/syslog-ng.conf; then
            echo "log {{ source($source_name); destination(remote_sct); }};" >> /etc/syslog-ng/syslog-ng.conf
        fi

        if [ ! -z "{hostname}" ]; then
            if grep "rewrite r_host" /etc/syslog-ng/syslog-ng.conf; then
                sed -i -r "s/rewrite r_host \\{{ set\\(\\"[^\\"]+\\"/rewrite r_host {{ set(\\"{hostname}\\"/" /etc/syslog-ng/syslog-ng.conf
            else
                echo "rewrite r_host {{ set(\\"{hostname}\\", value(\\"HOST\\")); }};" >>  /etc/syslog-ng/syslog-ng.conf
                sed -i -r "s/destination\\(remote_sct\\);[ \\t]*\\}};/destination\\(remote_sct\\); rewrite\\(r_host\\); \\}};/" /etc/syslog-ng/syslog-ng.conf
            fi
        fi
        """.format(host=host, port=port, hostname=hostname, throttle_per_second=throttle_per_second))


def configure_rsyslog_set_hostname_script(host: str) -> str:
    return dedent(f"""
    if ! grep "\\$LocalHostname {host}" /etc/rsyslog.conf; then
        echo "" >> /etc/rsyslog.conf
        echo "\\$LocalHostname {host}" >> /etc/rsyslog.conf
    fi
    """)


def configure_hosts_set_hostname_script(host: str) -> str:
    return f'grep -P "127.0.0.1[^\\\\n]+{host}" /etc/hosts || sed -ri "s/(127.0.0.1[ \\t]+' \
           f'localhost[^\\n]*)$/\\1\\t{host}/" /etc/hosts\n'


def configure_sshd_script():
    return dedent("""
    sed -i -e 's/^\*[[:blank:]]*soft[[:blank:]]*nproc[[:blank:]]*4096/*\t\tsoft\tnproc\t\tunlimited/' \
        /etc/security/limits.d/20-nproc.conf
    echo -e '*\t\thard\tnproc\t\tunlimited' >> /etc/security/limits.d/20-nproc.conf

    sed -i 's/#MaxSessions \(.*\)$/MaxSessions 1000/' /etc/ssh/sshd_config
    sed -i 's/#MaxStartups \(.*\)$/MaxStartups 60/' /etc/ssh/sshd_config
    sed -i 's/#LoginGraceTime \(.*\)$/LoginGraceTime 15s/' /etc/ssh/sshd_config
    """)


def restart_sshd_service():
    return "systemctl restart sshd\n"


def restart_rsyslog_service():
    return "systemctl restart rsyslog\n"


def restart_syslogng_service():
    return "systemctl restart syslog-ng\n"


def install_syslogng_service(return_status: bool = False):
    ending = dedent("""
    if [ -z "$SYSLOGNG_INSTALLED" ]; then
      exit 1
    else
      exit 0
    fi
    """) if return_status else ""
    return dedent("""
    SYSLOGNG_INSTALLED=""
    if yum --help 2>/dev/null 1>&2 ; then
      if ! yum list installed | grep syslog-ng; then
        yum install -y syslog-ng
      fi

      if yum list installed | grep syslog-ng; then
        SYSLOGNG_INSTALLED=1
      fi
    elif apt --help 2>/dev/null 1>&2 ; then
      if ! apt list --installed | grep syslog-ng; then
        apt update -y | apt update | true
        apt install -yf syslog-ng
      fi
      if apt list --installed | grep syslog-ng; then
        SYSLOGNG_INSTALLED=1
      fi
    else
      echo "Unsupported distro"
    fi
    {ending}
    """.format(ending=ending))
