from longevity_test import LongevityTest


class InMemoryLongevetyTest(LongevityTest):

    """
    :avocado: enable
    """

    def __init__(self, *args, **kwargs):
        super(InMemoryLongevetyTest, self).__init__(*args, **kwargs)

    def test_in_mem_longevity(self):
        self._pre_create_schema(in_memory=True)
        self.test_custom_time()
