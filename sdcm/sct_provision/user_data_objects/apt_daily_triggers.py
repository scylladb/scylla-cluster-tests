from dataclasses import dataclass

from sdcm.provision.common.utils import disable_daily_apt_triggers
from sdcm.sct_provision.user_data_objects import SctUserDataObject


@dataclass
class DisableAptTriggersUserDataObject(SctUserDataObject):
    @property
    def is_applicable(self) -> bool:
        return True

    @property
    def script_to_run(self) -> str:
        return disable_daily_apt_triggers()
