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
    if ! grep imjournalRatelimitInterval /etc/rsyslog.conf; then
        cat <<EOF >> /etc/rsyslog.conf
        #
        # The following configuration was added by SCT.
        #
        \$ModLoad imjournal
        \$imjournalRatelimitInterval {interval}
        \$imjournalRatelimitBurst {burst}
    EOF
    fi
    """)


def configure_rsyslog_target_script(host: str, port: int) -> str:
    return f'echo "action(type=\\"omfwd\\" Target=\\"{host}\\" Port=\\"{port}\\" Protocol=\\"tcp\\" ' \
           f'TCP_Framing=\\"octet-counted\\" Template=\\"RSYSLOG_SyslogProtocol23Format\\")" >> /etc/rsyslog.conf\n'


def configure_syslogng_target_script(host: str, port: int, throttle_per_second: int, hostname: str = "") -> str:
    return dedent("""
        source_name=`cat /etc/syslog-ng/syslog-ng.conf | tr -d "\\n" | tr -d "\\r" | sed -r "s/\\}};/\\}};\\n/g; \
        s/source /\\nsource /g" | grep -P "^source.*system\\(\\)" | cut -d" " -f2`
        disk_buffer_option=""
        if syslog-ng -V | grep disk; then
            disk_buffer_option="disk-buffer(
                    mem-buf-size(1048576)
                    disk-buf-size(104857600)
                    reliable(yes)
                    dir(\\\"/tmp\\\")
                )"
        fi

        if grep -P "keep-timestamp\\([^)]+\\)" /etc/syslog-ng/syslog-ng.conf; then
            sed -i -r "s/keep-timestamp([ ]*yes[ ]*)/keep-timestamp(no)/g" /etc/syslog-ng/syslog-ng.conf
        else
            sed -i -r "s/([ \t]*options[ \t]*\\\\{{)/\\\\1\\n  keep-timestamp(no);\\n/g" /etc/syslog-ng/syslog-ng.conf
        fi

        if ! grep "destination remote_sct" /etc/syslog-ng/syslog-ng.conf; then
            cat <<EOF >>/etc/syslog-ng/syslog-ng.conf
        destination remote_sct {{
            syslog(
                "{host}"
                transport("tcp")
                port({port})
                throttle({throttle_per_second})
                $disk_buffer_option
            );
        }};

        EOF
        fi

        if ! grep -P "log {{.*destination\\\\(remote_sct\\\\)" /etc/syslog-ng/syslog-ng.conf; then
            echo "
        filter filter_sct {{
            # filter audit out
            not program(\\"^audit\\");
        }};
            " >> /etc/syslog-ng/syslog-ng.conf
            echo "log {{ source($source_name); filter(filter_sct); destination(remote_sct); }};" >> /etc/syslog-ng/syslog-ng.conf
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


def configure_rsyslog_set_hostname_script(hostname: str) -> str:
    return dedent(f"""
    if grep "\\$LocalHostname {hostname}" /etc/rsyslog.conf; then
        sed -ei "s/\\$LocalHostname  \(.*\)$/\\$LocalHostname  {hostname}/" /etc/rsyslog.conf || true
    else
        echo "" >> /etc/rsyslog.conf
        echo "\\$LocalHostname {hostname}" >> /etc/rsyslog.conf
    fi
    """)


def configure_hosts_set_hostname_script(hostname: str) -> str:
    return f'grep -P "127.0.0.1[^\\\\n]+{hostname}" /etc/hosts || sed -ri "s/(127.0.0.1[ \\t]+' \
           f'localhost[^\\n]*)$/\\1\\t{hostname}/" /etc/hosts\n'


def configure_sshd_script():
    return dedent("""
    if [ -f "/etc/security/limits.d/20-nproc.conf" ]; then
        sed -i -e "s/^\*[[:blank:]]*soft[[:blank:]]*nproc[[:blank:]]*.*/*\t\tsoft\tnproc\t\tunlimited/" \
    /etc/security/limits.d/20-nproc.conf || true
    else
        echo "*    hard    nproc    unlimited" > /etc/security/limits.d/20-nproc.conf || true
    fi

    sed -i "s/#MaxSessions \(.*\)$/MaxSessions 1000/" /etc/ssh/sshd_config || true
    sed -i "s/#MaxStartups \(.*\)$/MaxStartups 60/" /etc/ssh/sshd_config || true
    sed -i "s/#LoginGraceTime \(.*\)$/LoginGraceTime 15s/" /etc/ssh/sshd_config || true
    sed -i "s/#ClientAliveInterval \(.*\)$/ClientAliveInterval 60/" /etc/ssh/sshd_config || true
    sed -i "s/#ClientAliveCountMax \(.*\)$/ClientAliveCountMax 10/" /etc/ssh/sshd_config || true
    """)


def restart_sshd_service():
    return "systemctl restart sshd || systemctl restart ssh || true\n"


def restart_rsyslog_service():
    return "systemctl restart rsyslog || true\n"


def restart_syslogng_service():
    return "systemctl restart syslog-ng  || true\n"


def install_syslogng_service():
    return dedent("""\
        SYSLOG_NG_INSTALLED=""
        if yum --help 2>/dev/null 1>&2 ; then
            if rpm -q syslog-ng ; then
                rm /etc/syslog-ng/syslog-ng.conf  # Make sure we have default syslog-ng.conf
                yum reinstall -y syslog-ng
                SYSLOG_NG_INSTALLED=1
            else
                for n in 1 2 3 4 5 6 7 8 9; do # cloud-init is running it with set +o braceexpand
                    if yum install -y --downloadonly syslog-ng; then
                        break
                    fi
                done

                for n in 1 2 3; do # cloud-init is running it with set +o braceexpand
                    if yum install -y syslog-ng; then
                        SYSLOG_NG_INSTALLED=1
                        break
                    fi
                    sleep 10
                done
            fi
        elif apt-get --help 2>/dev/null 1>&2 ; then
            if dpkg-query --show syslog-ng ; then
                rm /etc/syslog-ng/syslog-ng.conf  # Make sure we have default syslog-ng.conf
                apt-get purge -o DPkg::Lock::Timeout=300 -y syslog-ng*
                DPKG_FORCE=confmiss apt-get --reinstall -o DPkg::Lock::Timeout=300 -y install syslog-ng
                SYSLOG_NG_INSTALLED=1
            else
                cat /etc/apt/sources.list
                for n in 1 2 3 4 5 6 7 8 9; do # cloud-init is running it with set +o braceexpand
                    if apt-get -y update ; then
                        break
                    fi
                    sleep 0.5
                done

                for n in 1 2 3; do # cloud-init is running it with set +o braceexpand
                    apt-get install -o DPkg::Lock::Timeout=300 -y syslog-ng || true
                    if dpkg-query --show syslog-ng ; then
                        SYSLOG_NG_INSTALLED=1
                        break
                    fi
                done
            fi
        else
            echo "Unsupported distro"
        fi
    """)


def install_docker_service():
    return dedent("""\
        # Install Docker

        for n in 1 2 3; do
            if bash -c "$(curl -fsSL get.docker.com --retry 5 --retry-max-time 300 -o get-docker.sh)"; then
                break
            fi
            sleep $(backoff $n)
        done

        for n in 1 2 3; do
            if sh get-docker.sh ; then
                break
            fi
            sleep $(backoff $n)
        done

        systemctl enable docker.service
        systemctl start docker.service
    """)
