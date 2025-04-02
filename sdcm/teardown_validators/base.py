from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from sdcm.sct_config import SCTConfiguration

if TYPE_CHECKING:
    from sdcm.tester import ClusterTester

LOGGER = logging.getLogger(__name__)


class TeardownValidator:
    validator_name = None  # must be defined by child classes

    def __init__(self, params: SCTConfiguration, tester: ClusterTester):
        self.params = params
        self.tester = tester
        self.configuration = params.get(f"teardown_validators.{self.validator_name}")
        self.is_enabled = self.configuration.get("enabled", False)
        if not self.is_enabled:
            self.validate = lambda: LOGGER.info("%s validator is disabled", self.validator_name)

    def validate(self):
        raise NotImplementedError()
