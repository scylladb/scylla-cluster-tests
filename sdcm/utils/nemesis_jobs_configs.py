NEMESIS_REQUIRED_ADDITIONAL_CONFIGS = {
    "StopStartInterfacesNetworkMonkey": ["configurations/nemesis/additional_configs/extra_interface_public.yaml"],
    "RandomInterruptionNetworkMonkey": ["configurations/nemesis/additional_configs/extra_interface_public.yaml"],
    "BlockNetworkMonkey": ["configurations/nemesis/additional_configs/extra_interface_public.yaml"],
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

NEMESIS_ADDITIONAL_PIPELINE_PARAMS = {
    "StopStartInterfacesNetworkMonkey": {"ip_ssh_connections": "public"},
    "RandomInterruptionNetworkMonkey": {"ip_ssh_connections": "public"},
    "BlockNetworkMonkey": {"ip_ssh_connections": "public"},
}
