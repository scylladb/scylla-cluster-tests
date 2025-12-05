from sdcm.remote import shell_script_cmd


NODE_EXPORTER_VERSION = "1.7.0"


class NodeExporterSetup:  # pylint: disable=too-few-public-methods
    @staticmethod
    def install(node: "BaseNode | None" = None, remoter: "Remoter | None" = None):
        assert node or remoter, "node or remoter much be pass to this function"
        if node:
            node.install_package("wget")
            remoter = node.remoter
        remoter.sudo(
            shell_script_cmd(f"""
            if ! id node_exporter > /dev/null 2>&1; then
                useradd -rs /bin/false node_exporter
            fi
            wget https://github.com/prometheus/node_exporter/releases/download/v{NODE_EXPORTER_VERSION}/node_exporter-{NODE_EXPORTER_VERSION}.linux-amd64.tar.gz
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
            ExecStart=/usr/local/bin/node_exporter

            [Install]
            WantedBy=multi-user.target
            EOM

            systemctl daemon-reload
            systemctl enable node_exporter.service
            systemctl start node_exporter.service
        """)
        )


class SyslogNgExporterSetup:  # pylint: disable=too-few-public-methods
    @staticmethod
    def install(node: "BaseNode | None" = None, remoter: "Remoter | None" = None):
        assert node or remoter, "node or remoter much be pass to this function"
        if node:
            node.install_package("wget")
            remoter = node.remoter
        remoter.sudo(
            shell_script_cmd("""
            wget https://github.com/brandond/syslog_ng_exporter/releases/download/0.1.0/syslog_ng_exporter
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
        )
