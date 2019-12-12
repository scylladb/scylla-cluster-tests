from longevity_test import LongevityTest


class InMemoryLongevityTest(LongevityTest):
    def test_in_mem_longevity(self):
        self._pre_create_schema(in_memory=True)
        self.test_custom_time()
