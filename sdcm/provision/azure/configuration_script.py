from sdcm.provision.common.configuration_script import ConfigurationScriptBuilder


class AzureConfigurationScript(ConfigurationScriptBuilder):

    def to_string(self) -> str:
        script = super().to_string()
        return script
