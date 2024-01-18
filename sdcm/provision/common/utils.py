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
            echo "
        filter filter_sct {{
            # introduced as part of https://github.com/scylladb/scylla-cluster-tests/pull/3241
            not message(\\".*workload prioritization - update_service_levels_from_distributed_data: an error occurred while retrieving configuration\\") and

            # see https://github.com/scylladb/scylla-cluster-tests/issues/3705 for reasons
            not message(\\"cdc - Could not update CDC description table with generation\\") and

            # harmless shutdown message - scylladb/scylladb#11969
            not message(\\"ldap_connection - Seastar read failed: std::system_error \\(error system:104, read: Connection reset by peer\\)\\") and

            # issue relate to shutdown - decided to ignore in scylladb/scylladb#15672
            not message(\\".*view update generator not plugged to push updates\\") and

            # TODO: remove below workaround after dropping support for Scylla 2023.1 and 5.2 (see scylladb/scylla#13538)
            not message(\\"remove failed: Directory not empty\\") and

            # scylladb/scylladb#12972
            not message(\\"raft - .* failed with: seastar::rpc::remote_verb_error \\(connection is closed\\)\\") and

            # TODO: remove when those issues are solved:
            # https://github.com/scylladb/scylladb/issues/16206
            # https://github.com/scylladb/scylladb/issues/16259
            # https://github.com/scylladb/scylladb/issues/15598
            not message(\\".*view - Error applying view update.*\\") and

            # The below ldap-connection-reset is dependent on https://github.com/scylladb/scylla-enterprise/issues/2710
            not message(\\".*ldap_connection - Seastar read failed: std::system_error \\(error system:104, recv: Connection reset by peer\\).*\\") and

            # This scylla WARNING includes "exception" word and reported as ERROR. To prevent it I add the subevent below and locate
            # it before DATABASE_ERROR. Message example:
            # storage_proxy - Failed to apply mutation from 10.0.2.108#8: exceptions::mutation_write_timeout_exception
            # (Operation timed out for system.paxos - received only 0 responses from 1 CL=ONE.)
            not message(\\"\\(mutation_write_|Operation timed out for system.paxos|Operation failed for system.paxos\\)\\") and

            # scylladb/scylladb#10011
            not message(\\"\\(unknown verb exception|unknown_verb_error\\)\\") and

            # scylladb/scylla-enterprise#2552
            not message(\\"Operation timed out for system_distributed.service_levels\\") and

            # scylladb/scylladb#9656
            not message(\\"exception \\\\"gate closed\\\\" in no_wait handler ignored\\") and

            # all those are of unknown origin, i.e. no scylla issues for those:
            not message(\\"cql_server - exception while processing connection:\\") and
            not message(\\"semaphore_timed_out\\") and
            not message(\\"scylla-server.service.*State .stop-sigterm. timed out\\") and
            not message(\\"cql_server - exception while processing connection: seastar::nested_exception \\(seastar::nested_exception\\)$\\") and
            not message(\\"compaction_stopped_exception\\") and
            not message(\\"rpc - client .*\\(connection dropped|fail to connect\\)\\") and
            not message(\\"Rate-limit: supressed\\") and
            not message(\\"Rate-limit: suppressed\\") and

            # storage_proxy warning we dont care about
            not message(\\".*storage_proxy.*abort_requested_exception\\");
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
