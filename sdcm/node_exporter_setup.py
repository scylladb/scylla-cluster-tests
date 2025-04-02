from sdcm.remote import shell_script_cmd


NODE_EXPORTER_VERSION = '1.8.2'


class NodeExporterSetup:
    @staticmethod
    def install(node: "BaseNode | None" = None, remoter: "Remoter | None" = None):  # noqa: F821
        assert node or remoter, "node or remoter much be pass to this function"
        if node:
            remoter = node.remoter
        remoter.sudo(shell_script_cmd(f"""
            if ! id node_exporter > /dev/null 2>&1; then
                useradd -rs /bin/false node_exporter
            fi
            curl -L -O https://github.com/prometheus/node_exporter/releases/download/v{NODE_EXPORTER_VERSION}/node_exporter-{NODE_EXPORTER_VERSION}.linux-amd64.tar.gz
            tar -xzvf node_exporter-{NODE_EXPORTER_VERSION}.linux-amd64.tar.gz
            mv node_exporter-{NODE_EXPORTER_VERSION}.linux-amd64/node_exporter /usr/local/bin

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
            ExecStart=/usr/local/bin/node_exporter --no-collector.interrupts --no-collector.hwmon --no-collector.bcache --no-collector.btrfs --no-collector.fibrechannel --no-collector.infiniband --no-collector.ipvs --no-collector.nfs --no-collector.nfsd --no-collector.powersupplyclass --no-collector.rapl --no-collector.tapestats --no-collector.thermal_zone --no-collector.udp_queues --no-collector.zfs

            [Install]
            WantedBy=multi-user.target
            EOM

            systemctl daemon-reload
            systemctl enable node_exporter.service
            systemctl start node_exporter.service
        """))
