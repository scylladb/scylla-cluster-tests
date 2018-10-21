from longevity_test import LongevityTest


class InMemoryLongevetyTest(LongevityTest):

    """
    :avocado: enable
    """

    def __init__(self, *args, **kwargs):
        super(InMemoryLongevetyTest, self).__init__(*args, **kwargs)

    def test_in_mem_longevity(self):
        self._pre_create_schema()
        self.alter_table_to_in_memory()
        self.db_cluster.restart_scylla()
        self.test_custom_time()
