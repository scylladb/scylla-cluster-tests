from longevity_test import LongevityTest

DICT_KEYSPACES_COMPRESSION = {'keyspace_lz4': 'LZ4', 'keyspace_deflate': 'Deflate', 'keyspace_snappy': 'Snappy'}
DICT_DEFAULT_BLOB_COLUMNS = {'"C0"': 'blob', '"C1"': 'blob', '"C2"': 'blob', '"C3"': 'blob', '"C4"': 'blob'}


class IcsLongevetyTest(LongevityTest):
    def _pre_create_schema_with_compaction(self, keyspace_num=1, scylla_encryption_options=None, single_column=False,  # pylint: disable=too-many-arguments
                                           compaction='IncrementalCompactionStrategy', compression=None,
                                           create_all_keyspaces_table=True):  # pylint: disable=too-many-arguments
        """
        For cases we are testing many keyspaces and tables, It's a possibility that we will do it better and faster than
        cassandra-stress.
        """
        columns = {'"C0"': 'blob'} if single_column else DICT_DEFAULT_BLOB_COLUMNS
        self.log.debug('Pre Creating Schema for c-s with {} keyspaces'.format(keyspace_num))
        for i in range(1, keyspace_num + 1):
            keyspace_name = 'keyspace{}'.format(i)
            self.create_keyspace(keyspace_name=keyspace_name, replication_factor=3)
            self.log.debug('{} Created'.format(keyspace_name))
            self.create_table(name='standard1', keyspace_name=keyspace_name, key_type='blob', read_repair=0.0,
                              compact_storage=True,
                              columns=columns, compaction=compaction, compression=compression,
                              scylla_encryption_options=scylla_encryption_options)
        if create_all_keyspaces_table:
            for keyspace_name, keyspace_compression in DICT_KEYSPACES_COMPRESSION.items():
                self.create_keyspace(keyspace_name=keyspace_name, replication_factor=3)
                self.log.debug('{} Created'.format(keyspace_name))
                self.create_table(name='standard1', keyspace_name=keyspace_name, key_type='blob', read_repair=0.0,
                                  compact_storage=True, columns=DICT_DEFAULT_BLOB_COLUMNS,
                                  compaction=compaction, compression=keyspace_compression,
                                  scylla_encryption_options=scylla_encryption_options)

    def test_ics_1_column_longevity(self):
        self._pre_create_schema_with_compaction(single_column=True)
        self.test_custom_time()

    def test_ics_longevity(self):
        self._pre_create_schema_with_compaction()
        self.test_custom_time()
