from typing import Union

from pydantic import Field

from sdcm.provision.aws.configuration_script import AWSConfigurationScriptBuilder
from sdcm.provision.common.user_data import UserDataBuilderBase
from sdcm.sct_config import SCTConfiguration


class AWSInstanceUserDataBuilder(UserDataBuilderBase):
    params: Union[SCTConfiguration, dict] = Field(as_dict=False)
    syslog_host_port: tuple[str, int] = None

    def to_string(self) -> str:
        post_boot_script = AWSConfigurationScriptBuilder(
            # Monitoring and loader nodes does not use additional interface
            aws_additional_interface=False,
            aws_ipv6_workaround=self.params.get('ip_ssh_connections') == 'ipv6',
            syslog_host_port=self.syslog_host_port,
            disable_ssh_while_running=True,
        ).to_string()
        return post_boot_script
