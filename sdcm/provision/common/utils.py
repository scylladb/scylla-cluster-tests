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
                    dir(\\\"/var/log\\\")
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


def configure_ssh_accept_rsa():
    return dedent("""
    if (( $(ssh -V 2>&1 | tr -d "[:alpha:][:blank:][:punct:]" | cut -c-2) >= 88 )); then
        systemctl stop sshd || true
        sed -i "s/#PubkeyAuthentication \(.*\)$/PubkeyAuthentication yes/" /etc/ssh/sshd_config || true
        sed -i -e "\\$aPubkeyAcceptedAlgorithms +ssh-rsa" /etc/ssh/sshd_config || true
        sed -i -e "\\$aHostKeyAlgorithms +ssh-rsa" /etc/ssh/sshd_config || true
        systemctl restart sshd || true
    fi
    """)


def restart_sshd_service():
    return "systemctl restart sshd || true\n"


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
                yum install -y epel-release
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
                apt-get purge -y syslog-ng*
                DPKG_FORCE=confmiss apt-get --reinstall -y install syslog-ng
                SYSLOG_NG_INSTALLED=1
            else
                cat /etc/apt/sources.list
                for n in 1 2 3 4 5 6 7 8 9; do # cloud-init is running it with set +o braceexpand
                    if apt-get -y update 2>&1 | tee /tmp/syslog_ng_install.output || grep NO_PUBKEY \
        /tmp/syslog_ng_install.output; then
                        break
                    fi
                done

                for n in 1 2 3; do # cloud-init is running it with set +o braceexpand
                    while ! find /proc/*/fd -lname /var/lib/dpkg/lock-frontend -exec false {} + -quit ; do
                        sleep 1
                    done
                    apt-get install -y syslog-ng || true
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


def configure_syslog_ng_send_to_monitoring(monitor_address):
    return dedent(f"""\
        cat <<EOF >>/etc/syslog-ng/conf.d/remote_monitoring.conf

        destination remote_monitoring {{
            syslog(
                "{monitor_address}"
                transport("tcp")
                port(1514)
                throttle(10000)
                disk-buffer (
                    mem-buf-size(1048576)
                    disk-buf-size(104857600)
                    reliable(yes)
                    dir("/var/log")
                )
            );
        }};
        log {{ source(s_src);  destination(remote_monitoring); }};

        EOF
    """)
