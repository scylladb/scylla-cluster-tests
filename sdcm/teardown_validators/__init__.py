from sdcm.teardown_validators.events import ErrorEventsValidator
from sdcm.teardown_validators.rackaware import RackawareValidator
from sdcm.teardown_validators.sstables import SstablesValidator
from sdcm.teardown_validators.protected_db_nodes_memory import ProtectedDbNodesMemoryValidator

teardown_validators_list = [SstablesValidator, ErrorEventsValidator,
                            RackawareValidator, ProtectedDbNodesMemoryValidator]
