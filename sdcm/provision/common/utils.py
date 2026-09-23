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

from sdcm.utils.curl import curl_with_retry

VECTOR_VERSION = "latest"
VECTOR_LATEST_SOURCE = "https://packages.timber.io/vector/latest"
# used when the latest alias cannot be resolved, so installation can still try the backup download source.
VECTOR_FALLBACK_VERSION = "0.58.0"
# package download mirrors; the shell expands $_vector_version before use.
VECTOR_PACKAGE_SOURCES = (
    "https://packages.timber.io/vector/$_vector_version",
    "https://github.com/vectordotdev/vector/releases/download/v$_vector_version",
)

VECTOR_DOWNLOAD_MAX_TIME = 90
VECTOR_DOWNLOAD_RETRY = 1
VECTOR_DOWNLOAD_RETRY_MAX_TIME = 200
# checksum files are tiny, so they use a shorter timeout window.
VECTOR_CHECKSUM_MAX_TIME = 20
VECTOR_CHECKSUM_RETRY_MAX_TIME = 45

VECTOR_INSTALL_ATTEMPTS = "1 2 3 4 5 6"


def configure_syslogng_target_script(hostname: str = "") -> str:
    return dedent(
        f"""
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
        """
    )


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
        cat > /etc/vector/vector.yaml <<'EOF'
sources:
    journald:
        type: journald
    vector_metrics:
        type: internal_metrics

transforms:
    filter_audit:
        inputs: [journald]
        type: filter
        condition: |
            !starts_with(to_string(.SYSLOG_IDENTIFIER) ?? "default", "AUDIT")

    filter_system_services:
        inputs: [filter_audit]
        type: filter
        condition: |
            identifier = to_string(.SYSLOG_IDENTIFIER) ?? ""
            identifier != "sshd" &&
            identifier != "systemd" &&
            identifier != "systemd-logind" &&
            identifier != "sudo" &&
            identifier != "dhclient"

    filter_verbose_scylla:
        inputs: [filter_system_services]
        type: filter
        condition: |
            message = to_string(.message) ?? ""
            !contains(message, "] compaction - [Compact") &&
            !contains(message, "] table - Done with off-strategy compaction") &&
            !contains(message, "] table - Starting off-strategy compaction") &&
            !contains(message, "] repair - Repair") &&
            !contains(message, "repair id [id=") &&
            !contains(message, "] stream_session - [Stream") &&
            !contains(message, "] sstable - Rebuilding bloom filter") &&
            !contains(message, "] storage_proxy - Exception when communicating with")

    filter_suppress_warnings:
        inputs: [filter_verbose_scylla]
        type: filter
        condition: |
            message = to_string(.message) ?? ""
            !(match(message, to_regex!("^WARNING.*[shard.*]")) || match(message, to_regex!("^!.*WARNING.*[shard.*]")))

sinks:
    sct-runner:
        type: vector
        inputs: [filter_suppress_warnings]
        address: {host}:{port}
        healthcheck: false
    prometheus:
        type: prometheus_exporter
        address: 0.0.0.0:9577
        inputs: [vector_metrics]
        healthcheck: false
EOF

        chmod 0755 /etc/vector
        chmod 0644 /etc/vector/vector.yaml

        systemctl restart vector || echo "WARNING: vector.service restart failed, will be reconfigured later by configure_remote_logging"
    """).format(host=host, port=port)


def configure_hosts_set_hostname_script(hostname: str) -> str:
    return (
        f'grep -P "127.0.0.1[^\\\\n]+{hostname}" /etc/hosts || sed -ri "s/(127.0.0.1[ \\t]+'
        f'localhost[^\\n]*)$/\\1\\t{hostname}/" /etc/hosts\n'
    )


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
    """Install the vector.dev logging agent from a downloaded package.

    This avoids OS repositories and the upstream bootstrap script.
    """
    # both downloads share the output path variable; the script sets $_vector_dst before each download
    download_kwargs = dict(
        silent=True,
        follow_redirects=True,
        fail_early=True,
        retry=VECTOR_DOWNLOAD_RETRY,
        output="$_vector_dst",
        extra_flags="-S",
    )
    package_curl = curl_with_retry(
        "$_vector_url",
        retry_max_time=VECTOR_DOWNLOAD_RETRY_MAX_TIME,
        max_time=VECTOR_DOWNLOAD_MAX_TIME,
        **download_kwargs,
    )
    checksum_curl = curl_with_retry(
        "$_vector_url",
        retry_max_time=VECTOR_CHECKSUM_RETRY_MAX_TIME,
        max_time=VECTOR_CHECKSUM_MAX_TIME,
        **download_kwargs,
    )
    resolve_curl = curl_with_retry(
        f"{VECTOR_LATEST_SOURCE}/$_vector_latest_pkg",
        silent=True,
        retry=VECTOR_DOWNLOAD_RETRY,
        retry_max_time=VECTOR_CHECKSUM_RETRY_MAX_TIME,
        max_time=VECTOR_CHECKSUM_MAX_TIME,
        output="/dev/null",
        extra_flags='-I -S -w "%{redirect_url}"',
    )

    sources = " ".join(f'"{source}"' for source in VECTOR_PACKAGE_SOURCES)
    install_attempts = VECTOR_INSTALL_ATTEMPTS
    last_install_attempt = install_attempts.split()[-1]

    return dedent(f"""\
        # vector.dev logging agent: downloaded directly, checksum verified.
        if vector --version > /dev/null 2>&1; then
            echo "vector is already installed, keeping the version the image ships"
        else
            vector_fail() {{
                echo "ERROR: vector.dev installation failed: $1"
                shift
                while [ $# -gt 0 ]; do
                    echo "  $1"
                    shift
                done
                exit 1
            }}

            vector_pkg_name() {{
                if [ "$_vector_fmt" = "rpm" ]; then
                    echo "vector-$1-1.$_vector_arch.rpm"
                else
                    echo "vector_$1-1_$_vector_arch.deb"
                fi
            }}

            vector_note_attempt() {{
                _vector_attempts="$_vector_attempts; $1"
            }}

            _vector_machine=$(uname -m)
            case "$_vector_machine" in
                x86_64)
                    _vector_rpm_arch=x86_64
                    _vector_deb_arch=amd64
                    ;;
                aarch64|arm64)
                    _vector_rpm_arch=aarch64
                    _vector_deb_arch=arm64
                    ;;
                *)
                    vector_fail "unsupported architecture $_vector_machine"
                    ;;
            esac

            if yum --help > /dev/null 2>&1; then
                _vector_fmt=rpm
                _vector_arch=$_vector_rpm_arch
                _vector_install_cmd="rpm -U --replacepkgs"
            elif apt-get --help > /dev/null 2>&1; then
                _vector_fmt=deb
                _vector_arch=$_vector_deb_arch
                _vector_install_cmd="dpkg -i"
            else
                vector_fail "neither yum nor apt-get is available"
            fi

            _vector_version={VECTOR_VERSION}
            if [ "$_vector_version" = "latest" ]; then
                _vector_latest_pkg=$(vector_pkg_name latest)
                _vector_redirect=$({resolve_curl} || true)
                # matched with a character class rather than an escape: a backslash here is read
                # by Python first, and single quotes would close the bash -cxe wrapper early
                _vector_version=$(echo "$_vector_redirect" | grep -oE "[0-9]+[.][0-9]+[.][0-9]+" | head -1)

                if [ -n "$_vector_version" ]; then
                    echo "vector.dev latest resolves to $_vector_version"
                else
                    echo "WARNING: cannot resolve the latest vector.dev release from {VECTOR_LATEST_SOURCE}"
                    echo "  got: $_vector_redirect"
                    echo "  falling back to {VECTOR_FALLBACK_VERSION}"
                    _vector_version={VECTOR_FALLBACK_VERSION}
                fi
            fi

            _vector_pkg=$(vector_pkg_name "$_vector_version")
            _vector_sums="vector-$_vector_version-SHA256SUMS"
            _vector_pkg_path="/tmp/$_vector_pkg"
            _vector_sums_path="/tmp/$_vector_sums"
            _vector_log=/tmp/vector-install.log
            _vector_attempts=""
            _vector_downloaded=0

            for _vector_base in {sources}; do
                rm -f "$_vector_pkg_path" "$_vector_sums_path"

                _vector_rc=0
                _vector_url="$_vector_base/$_vector_pkg"
                _vector_dst=$_vector_pkg_path
                {package_curl} || _vector_rc=$?
                if [ "$_vector_rc" -ne 0 ]; then
                    vector_note_attempt "$_vector_url: curl exit $_vector_rc"
                    continue
                fi

                _vector_rc=0
                _vector_url="$_vector_base/$_vector_sums"
                _vector_dst=$_vector_sums_path
                {checksum_curl} || _vector_rc=$?
                if [ "$_vector_rc" -ne 0 ]; then
                    vector_note_attempt "$_vector_url: curl exit $_vector_rc"
                    continue
                fi

                _vector_expected=$(grep " $_vector_pkg\\$" "$_vector_sums_path" | cut -d" " -f1)
                _vector_actual=$(sha256sum "$_vector_pkg_path" | cut -d" " -f1)
                if [ -z "$_vector_expected" ] || [ "$_vector_expected" != "$_vector_actual" ]; then
                    vector_note_attempt "$_vector_base/$_vector_pkg: sha256 $_vector_actual != $_vector_expected"
                    continue
                fi

                _vector_downloaded=1
                break
            done

            if [ "$_vector_downloaded" -ne 1 ]; then
                vector_fail \
                    "no source served a usable $_vector_pkg" \
                    "version $_vector_version, architecture $_vector_machine" \
                    "attempts:$_vector_attempts"
            fi

            _vector_install_rc=1
            rm -f "$_vector_log"
            for n in {install_attempts}; do
                _vector_install_rc=0
                echo "=== attempt $n: $_vector_install_cmd $_vector_pkg_path" >> "$_vector_log"
                $_vector_install_cmd "$_vector_pkg_path" >> "$_vector_log" 2>&1 || _vector_install_rc=$?
                if [ "$_vector_install_rc" -eq 0 ]; then
                    break
                fi
                if [ "$n" -lt {last_install_attempt} ]; then
                    sleep "$(backoff "$n")"
                fi
            done

            _vector_unit=""
            for _vector_candidate in /usr/lib/systemd/system/vector.service /lib/systemd/system/vector.service; do
                if [ -f "$_vector_candidate" ]; then
                    _vector_unit=$_vector_candidate
                    break
                fi
            done

            _vector_rc=0
            vector --version > /dev/null 2>&1 || _vector_rc=$?
            if [ "$_vector_install_rc" -ne 0 ] || [ "$_vector_rc" -ne 0 ] || [ -z "$_vector_unit" ]; then
                echo "ERROR: vector.dev installation failed: $_vector_pkg did not install cleanly"
                echo "  version $_vector_version, architecture $_vector_machine"
                echo "  install exit $_vector_install_rc, vector --version exit $_vector_rc, unit file found: $_vector_unit"
                echo "  source attempts:$_vector_attempts"
                echo "  install output:"
                cat "$_vector_log" || true
                exit 1
            fi

            rm -f "$_vector_pkg_path" "$_vector_sums_path"
        fi

        systemctl enable vector
    """)


def install_syslogng_exporter():
    exporter_curl = curl_with_retry(
        "https://github.com/brandond/syslog_ng_exporter/releases/download/0.1.0/syslog_ng_exporter",
        follow_redirects=True,
        extra_flags="-O",
    )
    return dedent(f"""\
    {exporter_curl}
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
        if [ ! -f /tmp/disable_daily_apt_triggers_done ] && [ "$smi_installed" = "false" ]; then
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
        dpkg -i --force-confold --force-confdef {pkg_path}

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

        # vector is intentionally not started — it is started by configure_vector_target_script only
        # after the SCT target config is written
        systemctl enable vector
    """)


def install_docker_service():
    get_docker_curl = curl_with_retry(
        "get.docker.com",
        silent=True,
        follow_redirects=True,
        fail_early=True,
        output="get-docker.sh",
        extra_flags="-S",
    )
    return dedent(f"""\
        # Install Docker

        for n in 1 2 3; do
            if {get_docker_curl}; then
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
        {{
          "registry-mirrors": [
            "https://mirror.gcr.io"
          ]
        }}
        EOF

        systemctl enable docker.service
        systemctl start docker.service
    """)
