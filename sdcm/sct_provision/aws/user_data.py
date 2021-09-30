import base64
import json
from typing import Union

from pydantic import Field

from sdcm.provision.aws.configuration_script import AWSConfigurationScriptBuilder
from sdcm.provision.common.user_data import UserDataBuilderBase, DataDeviceType, ScyllaUserDataBuilderBase
from sdcm.provision.scylla_yaml import ScyllaYaml
from sdcm.sct_config import SCTConfiguration


class ScyllaUserDataBuilder(ScyllaUserDataBuilderBase):
    params: Union[SCTConfiguration, dict] = Field(as_dict=False)
    cluster_name: str
    bootstrap: bool = Field(default=None, as_dict=False)
    old_format: bool = Field(default=False, as_dict=False)
    scylla_yaml_raw: ScyllaYaml = Field(default=None, as_dict=False)

    @property
    def scylla_yaml(self) -> dict:
        scylla_yaml = ScyllaYaml() if self.scylla_yaml_raw is None else self.scylla_yaml_raw
        scylla_yaml.cluster_name = self.cluster_name
        if self.bootstrap is not None:
            scylla_yaml.auto_bootstrap = self.bootstrap
        return scylla_yaml.dict(exclude_defaults=True, exclude_none=True, exclude_unset=True)

    @property
    def start_scylla_on_first_boot(self):
        return False

    @property
    def data_device(self) -> DataDeviceType:
        """
        Tell scylla setup to target non-nvme volumes for data devices if it is configured so
         and use nvme disks otherwise
        """
        if self.params.get("data_volume_disk_num") > 0:
            return DataDeviceType.ATTACHED.value
        return DataDeviceType.INSTANCE_STORE.value

    @property
    def post_configuration_script(self) -> str:
        post_boot_script = AWSConfigurationScriptBuilder(
            aws_additional_interface=self.params.get('extra_network_interface') or False,
            aws_ipv6_workaround=self.params.get('ip_ssh_connections') == 'ipv6',
            # rsyslog is not running at this point
            rsyslog_host_port=None,
        ).to_string()
        return base64.b64encode(post_boot_script.encode('utf-8')).decode('ascii')

    def to_string(self) -> str:
        return self.return_in_old_format() if self.old_format else self.return_in_new_format()

    def return_in_new_format(self) -> str:
        return json.dumps(self.dict(exclude_defaults=True, exclude_unset=True, exclude_none=True))

    def return_in_old_format(self) -> str:
        output = f'--clustername {self.cluster_name} --totalnodes 1 --stop-services'
        if self.bootstrap is not None:
            output += ' --bootstrap true' if self.bootstrap else ' --bootstrap false'
        if self.post_configuration_script:
            output += ' --base64postscript=' + self.post_configuration_script
        return output


class AWSInstanceUserDataBuilder(UserDataBuilderBase):
    params: Union[SCTConfiguration, dict] = Field(as_dict=False)

    def to_string(self) -> str:
        post_boot_script = AWSConfigurationScriptBuilder(
            # Monitoring and loader nodes does not use additional interface
            aws_additional_interface=False,
            aws_ipv6_workaround=self.params.get('ip_ssh_connections') == 'ipv6',
            # rsyslog is not running at this point
            rsyslog_host_port=None,
        ).to_string()
        return post_boot_script
