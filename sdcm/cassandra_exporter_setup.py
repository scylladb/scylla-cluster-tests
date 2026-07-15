from sdcm.remote import shell_script_cmd
from sdcm.utils.curl import curl_with_retry

CASSANDRA_EXPORTER_VERSION = "2.3.8"
CASSANDRA_EXPORTER_PORT = 8080


class CassandraExporterSetup:
    """Install and configure Criteo cassandra_exporter for Prometheus metrics.

    The exporter runs as a standalone Java daemon that connects to Cassandra's
    JMX port (7199) and exposes Prometheus metrics on port 8080.

    Reference: https://github.com/criteo/cassandra_exporter
    """

    @staticmethod
    def install(node: "BaseNode | None" = None, remoter: "Remoter | None" = None):  # noqa: F821
        assert node or remoter, "node or remoter must be passed to this function"
        if node:
            remoter = node.remoter
        download_url = (
            f"https://github.com/criteo/cassandra_exporter/releases/download/{CASSANDRA_EXPORTER_VERSION}/"
            f"cassandra_exporter-{CASSANDRA_EXPORTER_VERSION}.jar"
        )
        download_cmd = curl_with_retry(download_url, follow_redirects=True, output="/opt/cassandra_exporter.jar")
        remoter.sudo(
            shell_script_cmd(f"""
            # Download Criteo cassandra_exporter
            {download_cmd}

            # Create config file
            cat > /etc/cassandra_exporter.yml <<'EXPORTER_CONFIG'
            host: localhost:7199
            ssl: false
            listenPort: {CASSANDRA_EXPORTER_PORT}
            blacklist:
              - java:lang:memorypool:.*usagethreshold.*
              - .*:999thpercentile
              - .*:95thpercentile
              - .*:75thpercentile
              - .*:50thpercentile
            maxScrapFrequencyInSec:
              50:
                - .*
              300:
                - .*:snapshotssize.*
                - .*:## table size.*
                - .*:totalDiskSpaceUsed.*
                - .*:estimatedPartitionCount.*
            EXPORTER_CONFIG
            # fix indentation from heredoc
            sed -i 's/^            //' /etc/cassandra_exporter.yml

            # Create systemd service
            cat > /etc/systemd/system/cassandra_exporter.service <<'EOM'
            [Unit]
            Description=Cassandra Prometheus Exporter
            After=network.target cassandra.service
            Requires=cassandra.service

            [Service]
            Type=simple
            ExecStart=/usr/bin/java -jar /opt/cassandra_exporter.jar /etc/cassandra_exporter.yml
            Restart=on-failure
            RestartSec=10

            [Install]
            WantedBy=multi-user.target
            EOM
            # fix indentation from heredoc
            sed -i 's/^            //' /etc/systemd/system/cassandra_exporter.service

            systemctl daemon-reload
            systemctl enable cassandra_exporter.service
            systemctl start cassandra_exporter.service
        """),
            ignore_status=True,
        )
