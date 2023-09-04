NEMESIS_REQUIRED_ADDITIONAL_CONFIGS = {
    "StopStartInterfacesNetworkMonkey": ["configurations/network_config/two_interfaces.yaml"],
    "RandomInterruptionNetworkMonkey": ["configurations/network_config/two_interfaces.yaml"],
    "BlockNetworkMonkey": ["configurations/network_config/two_interfaces.yaml"],
    "SlaSevenSlWithMaxSharesDuringLoad": ["configurations/nemesis/additional_configs/sla_config.yaml"],
    "SlaReplaceUsingDropDuringLoad": ["configurations/nemesis/additional_configs/sla_config.yaml"],
    "SlaReplaceUsingDetachDuringLoad": ["configurations/nemesis/additional_configs/sla_config.yaml"],
    "SlaMaximumAllowedSlsWithMaxSharesDuringLoad": ["configurations/nemesis/additional_configs/sla_config.yaml"],
    "SlaIncreaseSharesDuringLoad": ["configurations/nemesis/additional_configs/sla_config.yaml"],
    "SlaIncreaseSharesByAttachAnotherSlDuringLoad": ["configurations/nemesis/additional_configs/sla_config.yaml"],
    "SlaDecreaseSharesDuringLoad": ["configurations/nemesis/additional_configs/sla_config.yaml"],
    "PauseLdapNemesis": ["configurations/ldap-authorization.yaml"],
    "ToggleLdapConfiguration": ["configurations/ldap-authorization.yaml"],
}

# TODO: this definition should be removed when network configuration new mechanism will be supported by all backends.
#  Now "ip_ssh_connections" is not supported for AWS and it is ignored.
#  Test communication address (ip_ssh_connections) is defined as "public" for the relevant pipelines in "two_interfaces.yaml"
NEMESIS_ADDITIONAL_PIPELINE_PARAMS = {
    "StopStartInterfacesNetworkMonkey": {
        "ip_ssh_connections": "public"
    },
    "RandomInterruptionNetworkMonkey": {
        "ip_ssh_connections": "public"
    },
    "BlockNetworkMonkey": {
        "ip_ssh_connections": "public"
    },
}
