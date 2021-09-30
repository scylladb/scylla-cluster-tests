
import abc
from typing import Dict, Any, List, Union

from pydantic import BaseModel  # pylint: disable=no-name-in-module


TagsType = Dict[str, str]


class ProvisionParamsBase(BaseModel):  # pylint: disable=too-few-public-methods
    name: str
    region_name: str
    availability_zone: str
    duration: float = None
    price: float = None


class ProvisionGuaranteedParams(ProvisionParamsBase):  # pylint: disable=too-few-public-methods
    """
    Provision request parameters for instances that COULD NOT BE REVOKED by cloud provider
    Price for such instances are usually higher
    """


class ProvisionNotGuaranteedParams(ProvisionParamsBase):  # pylint: disable=too-few-public-methods
    """
    Provision request parameters for instances that COULD BE REVOKED by cloud provider
    Price for such instances are usually lower
    """


class InstanceParamsBase(BaseModel):  # pylint: disable=too-few-public-methods
    """
    Base class for instance parameters
    """


class ProvisionerBase(metaclass=abc.ABCMeta):  # pylint: disable=too-few-public-methods
    """
    Base class for provisioner - a class that provide API to provision instances
    """
    @abc.abstractmethod
    def provision(  # pylint: disable=too-many-arguments
            self,
            provision_parameters: ProvisionParamsBase,
            instance_parameters: InstanceParamsBase,
            count: int,
            tags: Union[List[TagsType], TagsType] = None,
            names: List[str] = None) -> List[Any]:
        pass
