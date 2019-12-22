from ics_space_amplification_test import IcsSpaceAmplificationTest
from longevity_test import LongevityTest


class IcsLongevityTest(LongevityTest):

    def _pre_create_schema_with_compaction(self):
        IcsSpaceAmplificationTest.pre_create_schema_with_compaction(self)

    def test_ics_longevity(self):
        self._pre_create_schema_with_compaction()
        self.test_custom_time()
