import abc

from azure.mgmt.network.v2021_02_01.models import NetworkProfile
from sdcm.provision.common.builders import AttrBuilder
from azure.mgmt.compute.v2021_07_01.models import HardwareProfile, StorageProfile, OSProfile


class AzureInstanceParamsBuilderBase(AttrBuilder, metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def hardware_profile(self) -> HardwareProfile:  # pylint: disable=invalid-name
        pass

    @property
    @abc.abstractmethod
    def storage_profile(self) -> StorageProfile:  # pylint: disable=invalid-name
        pass

    @property
    @abc.abstractmethod
    def network_profile(self) -> NetworkProfile:  # pylint: disable=invalid-name
        pass

    @property
    @abc.abstractmethod
    def os_profile(self) -> OSProfile:  # pylint: disable=invalid-name
        pass
