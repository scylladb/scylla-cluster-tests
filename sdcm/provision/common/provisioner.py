
import abc
from typing import Dict, Any, List, Union

from pydantic import BaseModel  # pylint: disable=no-name-in-module


TagsType = Dict[str, str]


class ProvisionParameters(BaseModel):  # pylint: disable=too-few-public-methods
    name: str  # Name of the parameter needed only to make distinction between the parameter instances
    region_name: str
    availability_zone: str
    spot: bool  # Signals on whether instance could be revoked by cloud provider
    duration: float = None  # Tells cloud provider on how much time you lock provisioned instances


class InstanceParamsBase(BaseModel):  # pylint: disable=too-few-public-methods
    """
    Base class for instance parameters
    """


class InstanceProvisionerBase(BaseModel, metaclass=abc.ABCMeta):  # pylint: disable=too-few-public-methods
    """
    Base class for provisioner - a class that provide API to provision instances
    """
    @abc.abstractmethod
    def provision(  # pylint: disable=too-many-arguments
            self,
            provision_parameters: ProvisionParameters,
            instance_parameters: InstanceParamsBase | List[InstanceParamsBase],
            count: int,
            tags: Union[List[TagsType], TagsType] = None,
            names: List[str] = None) -> List[Any]:
        pass
