from sdcm.remote import shell_script_cmd


NODE_EXPORTER_VERSION = '1.5.0'


class NodeExporterSetup:  # pylint: disable=too-few-public-methods
    @staticmethod
    def install(node):
        node.install_package('wget')
        node.remoter.sudo(shell_script_cmd(f"""
            useradd -rs /bin/false node_exporter
            wget https://github.com/prometheus/node_exporter/releases/download/v{NODE_EXPORTER_VERSION}/node_exporter-{NODE_EXPORTER_VERSION}.linux-amd64.tar.gz
            tar -xzvf node_exporter-{NODE_EXPORTER_VERSION}.linux-amd64.tar.gz
            mv node_exporter-{NODE_EXPORTER_VERSION}.linux-amd64/node_exporter /usr/local/bin

            cat <<EOM >> /etc/systemd/system/node_exporter.service
            [Unit]
            Description=Node Exporter
            After=network.target

            [Service]
            User=node_exporter
            Group=node_exporter
            Type=simple
            ExecStart=/usr/local/bin/node_exporter

            [Install]
            WantedBy=multi-user.target
            EOM

            systemctl daemon-reload
            systemctl enable node_exporter.service
            systemctl start node_exporter.service
        """))
