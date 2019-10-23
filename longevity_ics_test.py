from longevity_test import LongevityTest


class IcsLongevetyTest(LongevityTest):
    def _pre_create_schema(self, keyspace_num=1, scylla_encryption_options=None, single_column=False, compaction='IncrementalCompactionStrategy'):
        """
        For cases we are testing many keyspaces and tables, It's a possibility that we will do it better and faster than
        cassandra-stress.
        """
        columns = {'"C0"': 'blob'} if single_column else {'"C0"': 'blob', '"C1"': 'blob', '"C2"': 'blob', '"C3"': 'blob', '"C4"': 'blob'}
        self.log.debug('Pre Creating Schema for c-s with {} keyspaces'.format(keyspace_num))
        for i in xrange(1, keyspace_num+1):
            keyspace_name = 'keyspace{}'.format(i)
            self.create_keyspace(keyspace_name=keyspace_name, replication_factor=3)
            self.log.debug('{} Created'.format(keyspace_name))
            self.create_table(name='standard1', keyspace_name=keyspace_name, key_type='blob', read_repair=0.0, compact_storage=True,
                              columns=columns, compaction=compaction,
                              scylla_encryption_options=scylla_encryption_options)

    def test_ics_single_column_longevity(self):
        self._pre_create_schema(single_column=True)
        self.test_custom_time()

    def test_ics_longevity(self):
        self._pre_create_schema()
        self.test_custom_time()
