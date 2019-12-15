from longevity_test import LongevityTest


class InMemoryLongevityTest(LongevityTest):
    def __init__(self, *args, **kwargs):
        super(InMemoryLongevityTest, self).__init__(*args, **kwargs)

    def test_in_mem_longevity(self):
        self._pre_create_schema(in_memory=True)
        self.test_custom_time()
