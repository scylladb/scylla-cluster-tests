import logging

from sdcm.cluster import BaseCluster
from sdcm.sct_config import SCTConfiguration

LOGGER = logging.getLogger(__name__)


class TeardownValidator:  # pylint: disable=too-few-public-methods
    validator_name = None  # must be defined by child classes

    def __init__(self, params: SCTConfiguration, cluster: BaseCluster):
        self.params = params
        self.cluster = cluster
        self.configuration = params.get(f"teardown_validators.{self.validator_name}")
        self.is_enabled = self.configuration.get("enabled", False)
        if not self.is_enabled:
            self.validate = lambda: LOGGER.info("%s validator is disabled", self.validator_name)

    def validate(self):  # pylint: disable=method-hidden
        raise NotImplementedError()
