from sdcm.remote import shell_script_cmd
from sdcm.utils.curl import curl_with_retry


NODE_EXPORTER_VERSION = "1.8.2"

NODE_EXPORTER_DEFAULT_PORT = 9100
NODE_EXPORTER_PORTS = range(NODE_EXPORTER_DEFAULT_PORT, NODE_EXPORTER_DEFAULT_PORT + 20)

# Prints `running <port>` for the first node_exporter that already serves one of NODE_EXPORTER_PORTS (a distro
# package, or the one set up for another node on the same host), otherwise `free <port>` for the first port no
# one listens on
NODE_EXPORTER_PORT_PROBE = f"""
    free=
    for port in $(seq {NODE_EXPORTER_PORTS.start} {NODE_EXPORTER_PORTS.stop - 1}); do
        if [ -n "$(ss -Hltn sport = :$port)" ]; then
            if curl -s --max-time 2 http://127.0.0.1:$port/metrics | grep -q ^node_exporter_build_info; then
                echo running $port
                exit 0
            fi
        elif [ -z "$free" ]; then
            free=$port
        fi
    done
    echo free $free
"""

NODE_EXPORTER_ARCHITECTURES = {
    "x86_64": "amd64",
    "amd64": "amd64",
    "aarch64": "arm64",
    "arm64": "arm64",
}


class NodeExporterSetup:
    @staticmethod
    def install(node: "BaseNode | None" = None, remoter: "Remoter | None" = None) -> int:  # noqa: F821
        """Make a node_exporter serve the host and return its port.

        A node_exporter that already runs is reused, whatever its version and collectors: the dashboards read the
        same metric names. Otherwise SCT installs its own on the first free port.
        """
        assert node or remoter, "node or remoter much be pass to this function"
        if node:
            remoter = node.remoter
        state, _, port = remoter.run(
            shell_script_cmd(NODE_EXPORTER_PORT_PROBE, quote="'"), verbose=False
        ).stdout.partition(" ")
        if not port.strip():
            raise RuntimeError(f"No free port for node_exporter in {NODE_EXPORTER_PORTS}")
        port = int(port)
        if state == "running":
            remoter.log.info("Reusing the node_exporter that already serves port %s", port)
            return port
        machine = remoter.run("uname -m", verbose=False).stdout.strip()
        if not (arch := NODE_EXPORTER_ARCHITECTURES.get(machine)):
            raise ValueError(
                f"node_exporter has no release for machine '{machine}'. "
                f"Known values: {sorted(NODE_EXPORTER_ARCHITECTURES)}"
            )
        release = f"node_exporter-{NODE_EXPORTER_VERSION}.linux-{arch}"
        tarball = f"{release}.tar.gz"
        download_url = (
            f"https://github.com/prometheus/node_exporter/releases/download/v{NODE_EXPORTER_VERSION}/{tarball}"
        )
        download_cmd = curl_with_retry(download_url, retry=8, follow_redirects=True, fail_early=True, output=tarball)
        remoter.sudo(
            shell_script_cmd(f"""
            if ! id node_exporter > /dev/null 2>&1; then
                useradd -rs /bin/false node_exporter
            fi
            {download_cmd}
            tar -xzvf {tarball}
            mv {release}/node_exporter /usr/local/bin
            # Restore SELinux context so the binary can be executed as a service on RHEL-based systems
            if command -v restorecon > /dev/null 2>&1; then
                restorecon -v /usr/local/bin/node_exporter
            fi

            if [ -e /etc/systemd/system/node_exporter.service ]; then
                rm /etc/systemd/system/node_exporter.service
            fi

            cat <<EOM >> /etc/systemd/system/node_exporter.service
            [Unit]
            Description=Node Exporter
            After=network.target

            [Service]
            User=node_exporter
            Group=node_exporter
            Type=simple
            ExecStart=/usr/local/bin/node_exporter --web.listen-address=:{port} --no-collector.interrupts --no-collector.hwmon --no-collector.bcache --no-collector.btrfs --no-collector.fibrechannel --no-collector.infiniband --no-collector.ipvs --no-collector.nfs --no-collector.nfsd --no-collector.powersupplyclass --no-collector.rapl --no-collector.tapestats --no-collector.thermal_zone --no-collector.udp_queues --no-collector.zfs

            [Install]
            WantedBy=multi-user.target
            EOM

            systemctl daemon-reload
            systemctl enable node_exporter.service
            systemctl start node_exporter.service
        """),
            retry=3,
        )
        return port
