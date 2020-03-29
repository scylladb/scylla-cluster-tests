#
# This is stress longevity test that runs light weight transactions in parallel with differnt node operations:
# disruptive and not disruptive
#
# Schema definition yaml: data_dir/c-s_lwt_basic.yaml
#
# After the test is finished will be performed the data validation.
#
# 2 kinds of LWT updated will be run:
#
# update one column:
#    set lwt_indicator=30000000,
#    condition: for all rows where if lwt_indicator < 0
#
# update two columns:
#    set lwt_indicator=20000000 and author='text',
#    condition: for all rows where if lwt_indicator > 0 and lwt_indicator <= 1000000 and author != 'text'
#
# Additional expected that all rows where lwt_indicator > 1000000 and lwt_indicator < 20000000 won't be updated
#
# Based on this 3 types of validation will be performed:
#
# - Rows which are expected to stay intact, are not updated
# - Rows for a certain row range, lwt_indicator is changed to 30000000
# - Rows for another row range lwt_indicator is changed to 20000000
#
#
# ***Validation implementation***
#
# 1. Rows which are expected to stay intact.
#    To be able to validate that, I added a MV called blogposts_not_updated_lwt_indicator
#
#     - create MATERIALIZED VIEW blogposts_not_updated_lwt_indicator as select lwt_indicator, author from blogposts
#       where domain is not null and published_date is not null and lwt_indicator > 1000000 and lwt_indicator < 20000000
#       PRIMARY KEY(lwt_indicator, domain, published_date);
#     This MV hold all rows that shouldn't be updated.
#
#     Once the prepare_write_cmd part will be completed, all data from the view
#     blogposts_not_updated_lwt_indicator will be copied to a side table called
#     blogposts_not_updated_lwt_indicator_expect (created by test). This table uses as the expected data for this
#     validation.
#
#     When test is finished, the test checks that data in the blogposts_not_updated_lwt_indicator and
#     blogposts_not_updated_lwt_indicator_expect will be same.
#
# 2. For the rows where lwt_indicator was changed to "30000000", the data validation behaves as follow:
#
#     Two more Materialized View are added.
#     First one holds rows that candidates for this update (before the update):
#
#     create MATERIALIZED VIEW blogposts_update_one_column_lwt_indicator as select domain, lwt_indicator, author
#     from blogposts where domain is not null and published_date is not null and lwt_indicator < 0
#     PRIMARY KEY(lwt_indicator, domain, published_date);
#     Second one holds rows with lwt_indicator=30000000 (means - after update):
#
#     create MATERIALIZED VIEW blogposts_update_one_column_lwt_indicator_upd as select lwt_indicator, author
#     from blogposts where domain is not null and published_date is not null and lwt_indicator = 30000000
#     PRIMARY KEY(lwt_indicator, domain, published_date);
#     Ideally, The view blogposts_update_one_column_lwt_indicator should be empty after the update and the view
#     blogposts_update_one_column_lwt_indicator_upd will hold same amount of data as
#     blogposts_update_one_column_lwt_indicator had before the update.
#
#     However, due to the fact we cannot control the c-s workload to go over all the rows (should visit all the rows
#     with lwt_indicator negative value), for now it's just validated that blogposts_update_one_column_lwt_indicator_upd
#     has some records.
#     (It's unreasonable that the workload won't visit any row with negative value)
#
# 3. For the rows where lwt_indicator is changed to "20000000", the data validation the data validation behaves as
#    follow:
#
#     Two more Materialized View are added.
#     First one holds rows that are candidates for this update (before update):
#
#      create MATERIALIZED VIEW blogposts_update_two_columns_lwt_indicator as select lwt_indicator, author
#      from blogposts where domain is not null and published_date is not null and lwt_indicator > 0 and
#      lwt_indicator <= 1000000 PRIMARY KEY(lwt_indicator, domain, published_date);
#     Second one holds rows with lwt_indicator=20000000 (means - after the update):
#
#     create MATERIALIZED VIEW blogposts_update_two_columns_lwt_indicator_upd as select lwt_indicator, author
#     from blogposts where domain is not null and published_date is not null and lwt_indicator = 20000000
#     PRIMARY KEY(lwt_indicator, domain, published_date);

#     Ideally, the view blogposts_update_two_columns_lwt_indicator should be empty after the update and the view
#     blogposts_update_two_columns_lwt_indicator_upd will hold the same amount of data that as the view
#     blogposts_update_two_columns_lwt_indicator had before the update.
#
#     However, due to the same reason as above, the current validation is just that
#     blogposts_update_two_columns_lwt_indicator_upd has some records.
#

import re

from longevity_test import LongevityTest

from sdcm.utils.common import get_profile_content


class LWTLongevityTest(LongevityTest):
    def __init__(self, *args):
        super(LWTLongevityTest, self).__init__(*args)
        self._keyspace_name = None
        self._mv_for_not_updated_data = None
        self._expected_data_table = None
        self._validate_not_updated_data = True

    @property
    def keyspace_name(self):
        if not self._keyspace_name:
            prepare_write_cmd = self.params.get('prepare_write_cmd', default=[])
            profiles = [cmd for cmd in prepare_write_cmd if 'c-s_lwt' in cmd]
            if not profiles:
                return self._keyspace_name
            cs_profile = profiles[0]
            _, profile = get_profile_content(cs_profile)
            self._keyspace_name = profile['keyspace']

        return self._keyspace_name

    @property
    def mv_for_not_updated_data(self):
        """
        Get MV name that holds rows which are expected to stay intact
        """
        if not self._mv_for_not_updated_data:
            name_substr = '_not_updated'
            self._mv_for_not_updated_data = self.get_mv_name_from_profile(name_substr)
            return self._mv_for_not_updated_data

        return self._mv_for_not_updated_data

    @property
    def expected_data_table_name(self):
        """
        Table name for expected data (needs for validate rows which are expected to stay intact - implemented in
         validate_range_not_expected_to_change function)
        """
        if not self._expected_data_table:
            self._expected_data_table = '%s_expect' % self.mv_for_not_updated_data
        return self._expected_data_table

    def get_mv_name_from_profile(self, name_substr, cs_profile=None):
        mv_name = None

        if not cs_profile:
            prepare_write_cmd = self.params.get('prepare_write_cmd', default=[])
            cs_profile = [cmd for cmd in prepare_write_cmd if 'c-s_lwt' in cmd]

        if not cs_profile:
            return mv_name

        if not isinstance(cs_profile, list):
            cs_profile = [cs_profile]

        for profile in cs_profile:
            _, profile_content = get_profile_content(profile)
            mv_create_cmd = self.get_mv_cmd_from_profile(profile_content, name_substr)
            if mv_create_cmd:
                mv_name = self.get_mv_name_from_stress_cmd(mv_create_cmd, name_substr)
                break

        return mv_name

    @staticmethod
    def get_mv_cmd_from_profile(profile_content, name_substr):
        all_mvs = profile_content['extra_definitions']
        mv_cmd = [cmd for cmd in all_mvs if name_substr in cmd]

        mv_cmd = mv_cmd[0] if mv_cmd else ''
        return mv_cmd

    @staticmethod
    def get_mv_name_from_stress_cmd(mv_create_cmd, name_substr):
        find_mv_name = re.search(r'materialized view (.*%s.*) as' % name_substr, mv_create_cmd, re.I)
        return find_mv_name.group(1)

    def copy_expected_data(self):
        self.log.debug('Start copy test data')
        if not self.copy_view(src_keyspace=self.keyspace_name, src_view=self.mv_for_not_updated_data,
                              dest_keyspace=self.keyspace_name, dest_table=self.expected_data_table_name,
                              copy_data=True):
            self._validate_not_updated_data = False
            self.log.warning('Problem during copying expected data. '
                             'Data validation of not updated rows won\' be performed')
        self.log.debug('Finish copy test data')

    def run_prepare_write_cmd(self):
        super(LWTLongevityTest, self).run_prepare_write_cmd()
        self.log.info('Before copying data')
        self.copy_expected_data()

    def test_lwt_longevity(self):
        self.test_custom_time()
        self.validate_data()

    def validate_data(self):
        node = self.db_cluster.nodes[0]
        with self.cql_connection_patient(node, keyspace='cqlstress_lwt_example') as session:
            self.validate_range_not_expected_to_change(session=session)
            self.validate_updated_data(session=session)

    def validate_range_not_expected_to_change(self, session):
        """
        Part of data in the user profile table shouldn't be updated using LWT.
        This data will be saved in the materialized view with "not_updated" substring in the name
        After prepare write all data from this materialized view will be saved in the separate table as expected result.
        During stress (after prepare) LWT update statements will be run for a few hours.
        When updates will be finished this function verifies that data in "not_updated" MV and expected result table
        is same
        """
        if self._validate_not_updated_data and self.mv_for_not_updated_data and self.expected_data_table_name:
            self.log.debug('Verify not updated rows')
            # Get all rows, not use pagination
            session.default_fetch_size = 0

            actual_result = self.rows_to_list(session.execute("SELECT * FROM %s" % self.mv_for_not_updated_data))
            expected_result = self.rows_to_list(session.execute("SELECT * FROM %s" % self.expected_data_table_name))

            self.assertEqual(len(actual_result), len(expected_result),
                             'One or more rows are not as expected, suspected LWT wrong update. '
                             'Actual dataset length: {}, Expected dataset length: {}'.format(len(actual_result),
                                                                                             len(expected_result)))

            self.assertEqual(actual_result, expected_result,
                             'One or more rows are not as expected, suspected LWT wrong update')

    def validate_updated_data(self, session):
        """
        In user profile 'data_dir/c-s_lwt_basic.yaml' LWT updates the lwt_indicator and author columns with hard coded
        values. Validate, that we can find these values
        Two more materialized views are added. The first one holds rows that are candidates for the update
        (i.e. all rows before the update).
        The second one holds rows with lwt_indicator=30000000 (i.e. only the updated rows)
        """
        stress_cmd = self.params.get('stress_cmd', default=[])
        stress_read_cmd = self.params.get('stress_read_cmd', default=[])

        one_column_run_update = [cmd for cmd in stress_cmd+stress_read_cmd if 'lwt_update_one_column' in cmd]
        two_columns_run_update = [cmd for cmd in stress_cmd+stress_read_cmd if 'lwt_update_two_columns' in cmd]

        if one_column_run_update or two_columns_run_update:
            self.log.info('Verify that rows were updated')

            if one_column_run_update:
                expected_data_mv_name = self.get_mv_name_from_profile(name_substr='one_column_lwt_indicator_upd',
                                                                      cs_profile=one_column_run_update)
                result = session.execute('SELECT * from {name}'.format(name=expected_data_mv_name))

                self.assertTrue(result, 'Data in the column "lwt_indicator" was not updated by light weight '
                                        'transaction')

            if two_columns_run_update:
                expected_data_mv_name = self.get_mv_name_from_profile(name_substr='two_columns_lwt_indicator_upd',
                                                                      cs_profile=two_columns_run_update)
                result = session.execute('SELECT * from {name}'.format(name=expected_data_mv_name))

                self.assertTrue(result, 'Data in the columns "lwt_indicator" and "author" were not updated by light '
                                'weight transaction')
