from sdcm.teardown_validators.events import ErrorEventsValidator
from sdcm.teardown_validators.rackaware import RackawareValidator
from sdcm.teardown_validators.sstables import SstablesValidator

teardown_validators_list = [SstablesValidator, ErrorEventsValidator, RackawareValidator]
