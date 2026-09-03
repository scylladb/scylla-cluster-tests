from sdcm.remote import shell_script_cmd


NODE_EXPORTER_VERSION = "1.8.2"

NODE_EXPORTER_ARCHITECTURES = {
    "x86_64": "amd64",
    "amd64": "amd64",
    "aarch64": "arm64",
    "arm64": "arm64",
}


class NodeExporterSetup:
    @staticmethod
    def install(node: "BaseNode | None" = None, remoter: "Remoter | None" = None):  # noqa: F821
        assert node or remoter, "node or remoter much be pass to this function"
        if node:
            remoter = node.remoter
<<<<<<< HEAD
||||||| parent of 9896e9ba2 (feature(sizing): default loaders to Arm and match the image to the instance)
        tarball = f"node_exporter-{NODE_EXPORTER_VERSION}.linux-amd64.tar.gz"
        download_url = (
            f"https://github.com/prometheus/node_exporter/releases/download/v{NODE_EXPORTER_VERSION}/{tarball}"
        )
        download_cmd = curl_with_retry(download_url, retry=8, follow_redirects=True, fail_early=True, output=tarball)
=======
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
>>>>>>> 9896e9ba2 (feature(sizing): default loaders to Arm and match the image to the instance)
        remoter.sudo(
            shell_script_cmd(f"""
            if ! id node_exporter > /dev/null 2>&1; then
                useradd -rs /bin/false node_exporter
            fi
<<<<<<< HEAD
            curl -L -f --connect-timeout 10 --retry 8 --retry-max-time 300 -O https://github.com/prometheus/node_exporter/releases/download/v{NODE_EXPORTER_VERSION}/node_exporter-{NODE_EXPORTER_VERSION}.linux-amd64.tar.gz
            tar -xzvf node_exporter-{NODE_EXPORTER_VERSION}.linux-amd64.tar.gz
            mv node_exporter-{NODE_EXPORTER_VERSION}.linux-amd64/node_exporter /usr/local/bin
||||||| parent of 9896e9ba2 (feature(sizing): default loaders to Arm and match the image to the instance)
            {download_cmd}
            tar -xzvf {tarball}
            mv node_exporter-{NODE_EXPORTER_VERSION}.linux-amd64/node_exporter /usr/local/bin
            # Restore SELinux context so the binary can be executed as a service on RHEL-based systems
            if command -v restorecon > /dev/null 2>&1; then
                restorecon -v /usr/local/bin/node_exporter
            fi
=======
            {download_cmd}
            tar -xzvf {tarball}
            mv {release}/node_exporter /usr/local/bin
            # Restore SELinux context so the binary can be executed as a service on RHEL-based systems
            if command -v restorecon > /dev/null 2>&1; then
                restorecon -v /usr/local/bin/node_exporter
            fi
>>>>>>> 9896e9ba2 (feature(sizing): default loaders to Arm and match the image to the instance)

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
        """),
            retry=3,
        )
