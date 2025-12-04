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


def configure_syslogng_target_script(hostname: str = "") -> str:
    return dedent("""
        source_name=`cat /etc/syslog-ng/syslog-ng.conf | tr -d "\\n" | tr -d "\\r" | sed -r "s/\\}};/\\}};\\n/g; \
        s/source /\\nsource /g" | grep -P "^source.*system\\(\\)" | cut -d" " -f2`

        if grep -P "keep-timestamp\\([^)]+\\)" /etc/syslog-ng/syslog-ng.conf; then
            sed -i -r "s/keep-timestamp([ ]*yes[ ]*)/keep-timestamp(no)/g" /etc/syslog-ng/syslog-ng.conf
        else
            sed -i -r "s/([ \t]*options[ \t]*\\\\{{)/\\\\1\\n  keep-timestamp(no);\\n/g" /etc/syslog-ng/syslog-ng.conf
        fi

        write_syslog_ng_destination

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
        """.format(hostname=hostname))


def configure_vector_target_script(host: str, port: int) -> str:
    """Prepare vector configuration script with client-side log filtering.

    Configures vector to filter verbose logs before sending them to SCT, reducing memory pressure
    on database nodes and network resources usage.

    Filter Pipeline:
        journald > filter_audit > filter_system_services > filter_verbose_scylla > filter_suppress_warnings > sct-runner

    Filters:
        - filter_audit: remove audit logs
        - filter_system_services: remove unnecessary system services logs
        - filter_verbose_scylla: remove compaction/repair/streaming scylla logs
        - filter_suppress_warnings: remove Severity.SUPPRESS events
    """
    return dedent("""
        echo "
        sources:
            journald:
                type: journald
            vector_metrics:
                type: internal_metrics

        transforms:
            filter_audit:
                inputs:
                    - journald
                type: filter
                condition: |
                    !starts_with(to_string(.SYSLOG_IDENTIFIER) ?? \\"default\\", \\"AUDIT\\")

            filter_system_services:
                inputs:
                    - filter_audit
                type: filter
                condition: |
                    identifier = to_string(.SYSLOG_IDENTIFIER) ?? \\"\\"
                    identifier != \\"sshd\\" &&
                    identifier != \\"systemd\\" &&
                    identifier != \\"systemd-logind\\" &&
                    identifier != \\"sudo\\" &&
                    identifier != \\"dhclient\\"

            filter_verbose_scylla:
                inputs:
                    - filter_system_services
                type: filter
                condition: |
                    message = to_string(.message) ?? \\"\\"
                    !contains(message, \\"] compaction - [Compact\\") &&
                    !contains(message, \\"] table - Done with off-strategy compaction\\") &&
                    !contains(message, \\"] table - Starting off-strategy compaction\\") &&
                    !contains(message, \\"] repair - Repair\\") &&
                    !contains(message, \\"repair id [id=\\") &&
                    !contains(message, \\"] stream_session - [Stream\\") &&
                    !contains(message, \\"] sstable - Rebuilding bloom filter\\") &&
                    !contains(message, \\"] storage_proxy - Exception when communicating with\\")

            filter_suppress_warnings:
                inputs:
                    - filter_verbose_scylla
                type: filter
                condition: |
                    message = to_string(.message) ?? \\"\\"
                    !(match(message, to_regex!(\\"^WARNING.*[shard.*]\\")) || match(message, to_regex!(\\"^!.*WARNING.*[shard.*]\\")))

        sinks:
            sct-runner:
                type: vector
                inputs:
                    - filter_suppress_warnings
                address: {host}:{port}
                healthcheck: false
            prometheus:
                type: prometheus_exporter
                address: 0.0.0.0:9577
                inputs:
                  - vector_metrics
                healthcheck: false

        " > /etc/vector/vector.yaml

        systemctl kill -s HUP --kill-who=main vector.service
    """).format(host=host, port=port)


def configure_hosts_set_hostname_script(hostname: str) -> str:
    return f'grep -P "127.0.0.1[^\\\\n]+{hostname}" /etc/hosts || sed -ri "s/(127.0.0.1[ \\t]+' \
        f'localhost[^\\n]*)$/\\1\\t{hostname}/" /etc/hosts\n'


def configure_sshd_script():
    return dedent("""
    if [ -f "/etc/security/limits.d/20-nproc.conf" ]; then
        sed -i -e "s/^\\*[[:blank:]]*soft[[:blank:]]*nproc[[:blank:]]*.*/*\t\tsoft\tnproc\t\tunlimited/" \
    /etc/security/limits.d/20-nproc.conf || true
    else
        echo "*    hard    nproc    unlimited" > /etc/security/limits.d/20-nproc.conf || true
    fi

    sed -i "s/#MaxSessions \\(.*\\)$/MaxSessions 1000/" /etc/ssh/sshd_config || true
    sed -i "s/#MaxStartups \\(.*\\)$/MaxStartups 60/" /etc/ssh/sshd_config || true
    sed -i "s/#LoginGraceTime \\(.*\\)$/LoginGraceTime 15s/" /etc/ssh/sshd_config || true
    sed -i "s/#ClientAliveInterval \\(.*\\)$/ClientAliveInterval 60/" /etc/ssh/sshd_config || true
    sed -i "s/#ClientAliveCountMax \\(.*\\)$/ClientAliveCountMax 10/" /etc/ssh/sshd_config || true
    """)


def restart_sshd_service():
    return "systemctl restart sshd || systemctl restart ssh || true\n"


def restart_syslogng_service():
    return "systemctl restart syslog-ng  || true\n"


def configure_backoff_timeout():
    return dedent("""\
        backoff() {
            local attempt=$1
            local max_timeout=${2:-60}
            local base=${3:-5}
            local timeout

            timeout=$((attempt * base))
            if [ $timeout -gt $max_timeout ]; then
                timeout=$max_timeout
            fi
            echo $timeout
        }
    """)


def update_repo_cache():
    return dedent("""\
        if yum --help 2>/dev/null 1>&2 ; then
            echo "Cleaning yum cache..."
            yum clean all
            rm -rf /var/cache/yum/
        elif apt-get --help 2>/dev/null 1>&2 ; then
            echo "Cleaning apt cache..."
            apt-get clean all
            rm -rf /var/cache/apt/

            for n in 1 2 3 4 5 6 7 8 9; do
                if apt-get -y update; then
                    break
                fi
                sleep $(backoff $n)
            done
        else
            echo "Unsupported distro"
        fi
    """)


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
                    if yum install -y epel-release; then
                        break
                    fi
                    sleep $(backoff $n)
                done

                for n in 1 2 3 4 5 6 7 8 9; do # cloud-init is running it with set +o braceexpand
                    if yum install -y --downloadonly syslog-ng; then
                        break
                    fi
                    sleep $(backoff $n)
                done

                for n in 1 2 3; do # cloud-init is running it with set +o braceexpand
                    if yum install -y syslog-ng; then
                        SYSLOG_NG_INSTALLED=1
                        break
                    fi
                    sleep $(backoff $n)
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
                for n in 1 2 3; do # cloud-init is running it with set +o braceexpand
                    DEBIAN_FRONTEND=noninteractive apt-get install -o DPkg::Lock::Timeout=300 -y syslog-ng || true
                    if dpkg-query --show syslog-ng ; then
                        SYSLOG_NG_INSTALLED=1
                        break
                    fi
                    sleep $(backoff $n)
                done
            fi
        else
            echo "Unsupported distro"
        fi
    """)


def install_vector_service():
    return dedent("""\
        # install repo
        for n in 1 2 3; do # cloud-init is running it with set +o braceexpand
            if bash -c "$(curl -L https://setup.vector.dev)"; then
                break
            fi
            sleep $(backoff $n)
        done

        # install vector
        if yum --help 2>/dev/null 1>&2 ; then
            for n in 1 2 3; do # cloud-init is running it with set +o braceexpand
                if yum install -y vector; then
                    break
                fi
                sleep $(backoff $n)
            done
        elif apt-get --help 2>/dev/null 1>&2 ; then
            for n in 1 2 3; do # cloud-init is running it with set +o braceexpand
                DEBIAN_FRONTEND=noninteractive apt-get install -o DPkg::Lock::Timeout=300 -y vector || true
                if dpkg-query --show vector ; then
                    break
                fi
                sleep $(backoff $n)
            done
        else
            echo "Unsupported distro"
        fi

        systemctl enable vector
        systemctl start vector
    """)


def install_syslogng_exporter():
    return dedent("""\
    curl -L -O https://github.com/brandond/syslog_ng_exporter/releases/download/0.1.0/syslog_ng_exporter
    chmod +x syslog_ng_exporter
    mv syslog_ng_exporter /usr/local/bin

    if [ -e /etc/systemd/system/syslog_ng_exporter.service ]; then
        rm /etc/systemd/system/syslog_ng_exporter.service
    fi

    cat <<EOM >> /etc/systemd/system/syslog_ng_exporter.service
    [Unit]
    Description=Syslog-ng metrics Exporter
    Wants=network.target network-online.target
    After=network.target network-online.target

    [Service]
    Type=simple
    ExecStart=/usr/local/bin/syslog_ng_exporter
    StandardOutput=journal
    StandardError=journal
    Restart=on-failure

    [Install]
    WantedBy=multi-user.target
    EOM

    systemctl daemon-reload
    systemctl enable syslog_ng_exporter.service
    systemctl start syslog_ng_exporter.service
""")


def disable_daily_apt_triggers():
    return dedent("""\
    if apt-get --help >/dev/null 2>&1 ; then
        smi_installed=false
        dpkg -s scylla-machine-image &> /dev/null && smi_installed=true
        dpkg -s scylla-enterprise-machine-image &> /dev/null && smi_installed=true
        if [ ! -f /tmp/disable_daily_apt_triggers_done && ! $smi_installed ]; then
            rm -f /etc/apt/apt.conf.d/*unattended-upgrades /etc/apt/apt.conf.d/*auto-upgrades || true
            rm -f /etc/apt/apt.conf.d/*periodic /etc/apt/apt.conf.d/*update-notifier || true
            systemctl stop apt-daily.timer apt-daily-upgrade.timer apt-daily.service apt-daily-upgrade.service || true
            systemctl disable apt-daily.timer apt-daily-upgrade.timer apt-daily.service apt-daily-upgrade.service || true
            apt-get remove -o DPkg::Lock::Timeout=300 -y unattended-upgrades update-manager || true
            touch /tmp/disable_daily_apt_triggers_done
        fi
    fi
    """)


def configure_syslogng_destination_conf(host: str, port: int, throttle_per_second: int) -> str:
    return dedent("""
        write_syslog_ng_destination() {{
            disk_buffer_option=""
            if syslog-ng -V | grep -q disk; then
                disk_buffer_option="disk-buffer(
                    mem-buf-size(1048576)
                    disk-buf-size(104857600)
                    reliable(yes)
                    dir(\\\"/var/log\\\")
                )"
            fi

        cat <<EOF >/etc/syslog-ng/conf.d/remote_sct.conf
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
        }}
        """).format(host=host, port=port, throttle_per_second=throttle_per_second)


def configure_syslogng_file_source(log_file: str) -> str:
    """Configures an additional syslog-ng source for ScyllaDB logs from a file."""
    return dedent(f"""
        cat <<EOF >/etc/syslog-ng/conf.d/scylla_file_source.conf
        source s_scylla_file {{
            file("{log_file}" follow-freq(1) flags(no-parse));
        }};
        EOF

        echo "log {{ source(s_scylla_file); filter(filter_sct); destination(remote_sct); rewrite(r_host); }};" >> /etc/syslog-ng/syslog-ng.conf
    """)


def install_vector_from_local_pkg(pkg_path: str) -> str:
    """Install Vector from a local .deb package"""
    return dedent(f"""\
        dpkg -i {pkg_path}

{update_repo_cache()}
        for n in 1 2 3; do
            DEBIAN_FRONTEND=noninteractive apt-get install -o Dpkg::Options::=--force-confold -o Dpkg::Options::=--force-confdef -o DPkg::Lock::Timeout=300 -y vector || true
            if dpkg-query --show vector ; then
                break
            fi
            sleep $(backoff $n)
        done

        if ! dpkg-query --show vector ; then
            echo "ERROR: Failed to install vector package"
            exit 1
        fi

        systemctl enable vector
        systemctl start vector
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

        # Configure Docker to use Google Container Registry mirrors
        mkdir -p /etc/docker
        cat > /etc/docker/daemon.json <<EOF
        {
          "registry-mirrors": [
            "https://mirror.gcr.io"
          ]
        }
        EOF

        systemctl enable docker.service
        systemctl start docker.service
    """)
