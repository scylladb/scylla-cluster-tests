from typing import Union

from pydantic import Field

from sdcm.provision.common.configuration_script import ConfigurationScriptBuilder
from sdcm.provision.common.user_data import UserDataBuilderBase
from sdcm.sct_config import SCTConfiguration


class AzureInstanceUserDataBuilder(UserDataBuilderBase):
    params: Union[SCTConfiguration, dict] = Field(as_dict=False)
    syslog_host_port: tuple[str, int] = None

    def to_string(self) -> str:
        post_boot_script = ConfigurationScriptBuilder(
            syslog_host_port=self.syslog_host_port,
            disable_ssh_while_running=True,
        ).to_string()
        return post_boot_script