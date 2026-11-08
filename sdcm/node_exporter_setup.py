from textwrap import dedent

from sdcm.remote import shell_script_cmd
from sdcm.utils.curl import curl_with_retry

NODE_EXPORTER_VERSION = "1.8.2"

NODE_EXPORTER_DEFAULT_PORT = 9100
NODE_EXPORTER_PORTS = range(NODE_EXPORTER_DEFAULT_PORT, NODE_EXPORTER_DEFAULT_PORT + 20)

# Sets `state=running port=<port>` for the first node_exporter that already serves one of NODE_EXPORTER_PORTS at
# $host, the address Prometheus scrapes (a distro package, or the one set up for another node on the same host),
# otherwise `state=free port=<port>` for the first port no one listens on (an empty port when all are taken). An
# exporter bound elsewhere, e.g. to loopback, isn't reused.
NODE_EXPORTER_PORT_PROBE = dedent(f"""
    state=free
    port=
    for p in $(seq {NODE_EXPORTER_PORTS.start} {NODE_EXPORTER_PORTS.stop - 1}); do
        if [ -n "$(ss -Hltn sport = :$p)" ]; then
            if curl -s --max-time 2 http://$host:$p/metrics | grep -q ^node_exporter_build_info; then
                state=running
                port=$p
                break
            fi
        elif [ -z "$port" ]; then
            port=$p
        fi
    done
""")

# Serializes setup on a host, so a loader and a monitor sharing it don't install over each other
NODE_EXPORTER_LOCK = "/var/lock/sct-node-exporter.lock"

NODE_EXPORTER_ARCHITECTURES = {
    "x86_64": "amd64",
    "amd64": "amd64",
    "aarch64": "arm64",
    "arm64": "arm64",
}


class NodeExporterSetup:
    @staticmethod
    def install(
        node: "BaseNode | None" = None,  # noqa: F821
        remoter: "Remoter | None" = None,  # noqa: F821
        scrape_host: str | None = None,
    ) -> int:
        """Make a node_exporter serve the host and return its port.

        With `scrape_host` (URL form, IPv6 in brackets), a node_exporter that already answers there is reused, whatever
        its version and collectors: the dashboards read the same metric names. Otherwise SCT installs its own on the
        first free port. Without it, for callers whose scrape target is fixed, SCT installs on the default port.
        """
        assert node or remoter, "node or remoter much be pass to this function"
        if node:
            remoter = node.remoter
        if scrape_host:
            probe = f"host={scrape_host}\n{NODE_EXPORTER_PORT_PROBE}"
        else:
            probe = f"state=free\nport={NODE_EXPORTER_DEFAULT_PORT}\n"
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
        install = dedent(f"""
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

            cat <<EOM > /etc/systemd/system/node_exporter.service
            [Unit]
            Description=Node Exporter
            After=network.target

            [Service]
            User=node_exporter
            Group=node_exporter
            Type=simple
            ExecStart=/usr/local/bin/node_exporter --web.listen-address=:$port --no-collector.interrupts --no-collector.hwmon --no-collector.bcache --no-collector.btrfs --no-collector.fibrechannel --no-collector.infiniband --no-collector.ipvs --no-collector.nfs --no-collector.nfsd --no-collector.powersupplyclass --no-collector.rapl --no-collector.tapestats --no-collector.thermal_zone --no-collector.udp_queues --no-collector.zfs

            [Install]
            WantedBy=multi-user.target
            EOM

            systemctl daemon-reload
            systemctl enable node_exporter.service
            # restart, not start: a unit already running (one the probe missed) keeps its old port on start
            systemctl restart node_exporter.service
        """)
        # Probes under the lock: another node's setup on this host may have started an exporter meanwhile
        result = remoter.sudo(
            shell_script_cmd(
                f"exec 9>{NODE_EXPORTER_LOCK}\nflock 9\n{probe}"
                f'if [ "$state" = free ] && [ -n "$port" ]; then\n{install}fi\necho "$state $port"\n',
                quote="'",
            ),
            retry=3,
        )
        state, _, port = result.stdout.strip().splitlines()[-1].partition(" ")
        if not port:
            raise RuntimeError(f"No free port for node_exporter in {NODE_EXPORTER_PORTS}")
        if state == "running":
            remoter.log.info("Reusing the node_exporter that already serves port %s", port)
        return int(port)
