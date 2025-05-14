import base64
import json
import logging
from typing import Union, Any
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart

from pydantic import Field, computed_field

from sdcm.provision.aws.configuration_script import AWSConfigurationScriptBuilder
from sdcm.provision.common.user_data import UserDataBuilderBase, DataDeviceType, ScyllaUserDataBuilderBase, RaidLevelType
from sdcm.provision.network_configuration import is_ip_ssh_connections_ipv6, network_interfaces_count
from sdcm.provision.scylla_yaml import ScyllaYaml
from sdcm.sct_config import SCTConfiguration

LOGGER = logging.getLogger()


class ScyllaUserDataBuilder(ScyllaUserDataBuilderBase):
    params: Union[SCTConfiguration, dict] = Field(exclude=True)
    cluster_name: str
    bootstrap: bool = Field(default=None, exclude=True)
    user_data_format_version: str = Field(default='2', exclude=True)
    scylla_yaml_raw: ScyllaYaml = Field(default=None, exclude=True)
    syslog_host_port: tuple[str, int] | None = Field(default=None, exclude=True)
    test_config: Any = Field(exclude=True)

    @computed_field
    @property
    def scylla_yaml(self) -> dict:
        scylla_yaml = ScyllaYaml() if self.scylla_yaml_raw is None else self.scylla_yaml_raw
        scylla_yaml.cluster_name = self.cluster_name
        if self.bootstrap is not None:
            scylla_yaml.auto_bootstrap = self.bootstrap
        return scylla_yaml.model_dump(exclude_defaults=True, exclude_none=True, exclude_unset=True)

    @computed_field
    @property
    def start_scylla_on_first_boot(self) -> bool:
        return False

    @computed_field
    @property
    def data_device(self) -> DataDeviceType:
        """
        Tell scylla setup to target non-nvme volumes for data devices if it is configured so
         and use nvme disks otherwise
        """
        if self.params.get("data_volume_disk_num") > 0:
            return DataDeviceType.ATTACHED.value
        return DataDeviceType.INSTANCE_STORE.value

    @computed_field
    @property
    def raid_level(self) -> RaidLevelType:
        """
        Tell scylla setup to create RAID0 or RAID5 on disks
        """
        return self.params.get("raid_level") or RaidLevelType.RAID0

    @computed_field
    @property
    def post_configuration_script(self) -> str:
        post_boot_script = AWSConfigurationScriptBuilder(
            aws_additional_interface=network_interfaces_count(self.params) > 1,
            aws_ipv6_workaround=is_ip_ssh_connections_ipv6(self.params),
            syslog_host_port=self.syslog_host_port,
            logs_transport=self.params.get('logs_transport'),
            test_config=self.test_config,
        ).to_string()
        LOGGER.debug("post_boot_script: %s", post_boot_script)
        return base64.b64encode(post_boot_script.encode('utf-8')).decode('ascii')

    def to_string(self) -> str:
        match self.user_data_format_version:
            case '1':
                return self.return_in_format_v1()
            case '2':
                return self.return_in_format_v2()
            case '3':
                return self.return_in_format_v3()

    def return_in_format_v3(self) -> str:
        msg = MIMEMultipart()
        scylla_image_configuration = self.model_dump()
        scylla_image_configuration.pop('post_configuration_script', False)
        part = MIMEBase('x-scylla', 'json')
        part.set_payload(json.dumps(scylla_image_configuration, indent=4, sort_keys=True))
        part.add_header('Content-Disposition', 'attachment; filename="scylla_machine_image.json"')
        msg.attach(part)

        cloud_config = """
        #cloud-config
        cloud_final_modules:
        - [scripts-user, always]
        """
        part = MIMEBase('text', 'cloud-config')
        part.set_payload(cloud_config)
        part.add_header('Content-Disposition', 'attachment; filename="cloud-config.txt"')
        msg.attach(part)

        part = MIMEBase('text', 'x-shellscript')
        part.set_payload(base64.b64decode(self.post_configuration_script))
        part.add_header('Content-Disposition', 'attachment; filename="user-script.txt"')
        msg.attach(part)

        return str(msg)

    def return_in_format_v2(self) -> str:
        return json.dumps(self.model_dump(exclude_defaults=True, exclude_unset=True, exclude_none=True), indent=4, sort_keys=True)

    def return_in_format_v1(self) -> str:
        output = f'--clustername {self.cluster_name} --totalnodes 1 --stop-services'
        if self.bootstrap is not None:
            output += ' --bootstrap true' if self.bootstrap else ' --bootstrap false'
        if self.post_configuration_script:
            output += ' --base64postscript=' + self.post_configuration_script
        return output


class AWSInstanceUserDataBuilder(UserDataBuilderBase):
    params: Union[SCTConfiguration, dict] = Field(exclude=True)
    syslog_host_port: tuple[str, int] | None = None
    test_config: Any = Field(exclude=True)
    aws_additional_interface: bool = False

    def to_string(self) -> str:
        post_boot_script = AWSConfigurationScriptBuilder(
            # Monitoring and loader nodes does not use additional interface
            aws_additional_interface=self.aws_additional_interface,
            aws_ipv6_workaround=is_ip_ssh_connections_ipv6(self.params),
            logs_transport=self.params.get('logs_transport'),
            syslog_host_port=self.syslog_host_port,
            test_config=self.test_config,
        ).to_string()
        return post_boot_script
