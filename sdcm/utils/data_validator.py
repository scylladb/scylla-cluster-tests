# Data validation module may be used with cassandra-stress user profile only
#
# **************** Caution **************************************************************
# BE AWARE: During data validation all materialized views/expected table rows will be read into the memory
#           Be sure your dataset will be less then 2Gb.
# ****************************************************************************************
#
# Here is described Data validation module and requirements for user profile.
# Please, read the explanation and requirements
#
#
# 2 kinds of updates may be tested:
#
# 1. update rows
#   Example:
#   update one column:
#       set lwt_indicator=30000000,
#       condition: for all rows where if lwt_indicator < 0
#
#   update two columns:
#       set lwt_indicator=20000000 and author='text',
#       condition: for all rows where if lwt_indicator > 0 and lwt_indicator <= 1000000 and author != 'text'
#
# 2. delete rows
#   Example:
#   deletes:
#       delete from blogposts where
#       condition: lwt_indicator > 10000 and lwt_indicator < 100000
#
#
# And additional expected that all rows in the certain rand won't be updated
# Example:
#   where lwt_indicator > 1000000 and lwt_indicator < 20000000
#
# Based on this 3 types of validation will be performed:
#
# - Rows which are expected to stay intact, are not updated
#   REQUIREMENTS:
#     1. create view with substring "_not_updated" in the name. Example: blogposts_not_updated_lwt_indicator
#     2. view name length have to be less then 40 characters
# - Rows for a certain row range, that were updated
#   REQUIREMENTS:
#     1. create 2 views:
#            1 - first one holds rows that candidates for this update. View name length have to be less then 27
#                characters. Example: blogposts_update_one_column_lwt_indicator
#            2 - Second one holds rows after update.
#               The name of this view have to be as: <name of first view>+<_after_update>
#               Example: blogposts_update_one_column_lwt_indicator_after_update
#
# - Rows for another row range that part of them were deleted
#   REQUIREMENT:
#       with substring "_deletions" in the name should be created (example, blogposts_deletions)
#
#
# ***Validation implementation***
#
# 1. Rows which are expected to stay intact.
#    To be able to validate that, the MV with substring "_not_updated" in the name should be created (example,
#       blogposts_not_updated_lwt_indicator)
#
#    Example:
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
# 2. For the rows one or more columns were updated, the data validation behaves as follow:
#
#     Two more Materialized View have to be added added:
#       1 - First one holds rows that candidates for this update (before the update):
#           ****IMPORTANT: name of this view have no to be longer then 27 characters****
#       Example:
#           create MATERIALIZED VIEW blogposts_update_one_column_lwt_indicator as select domain, lwt_indicator, author
#           from blogposts where domain is not null and published_date is not null and lwt_indicator < 0
#           PRIMARY KEY(lwt_indicator, domain, published_date);
#
#       2 - Second one holds rows after update.
#           The name of this view have to be as: <name of first view>+<_after_update>
#       Example:
#           create MATERIALIZED VIEW blogposts_update_one_column_lwt_indicator_after_update as
#           select lwt_indicator, author from blogposts
#           where domain is not null and published_date is not null and
#           lwt_indicator = 30000000 PRIMARY KEY(lwt_indicator, domain, published_date);
#
#     Once the prepare_write_cmd part will be completed, all primary key data from the first view
#     will be copied to a side table called <view name>+<_expect> (example, blogposts_not_updated_lwt_indicator_expect)
#     that created by test. This table uses as the expected data for this validation.
#
#     Ideally, The view blogposts_update_one_column_lwt_indicator should be empty after the update and
#     the view blogposts_update_one_column_lwt_indicator_after_update will hold same amount of data as
#     blogposts_update_one_column_lwt_indicator had before the update.
#
#     However, due to the fact we cannot control the c-s workload to go over all the rows (should visit all the rows
#     with appropriate value), it's validated that first and second views together has same primary keys that in
#     expected data table
#
#
# 3. Rows which are deleted.
#    To be able to validate that, the MV with substring "_deletions" in the name should be created (example,
#       blogposts_deletions)
#
#   Example:
#     - create MATERIALIZED VIEW blogposts_deletions as select lwt_indicator, author from blogposts
#       where domain is not null and published_date is not null and lwt_indicator > 10000 and lwt_indicator < 100000
#       PRIMARY KEY(lwt_indicator, domain, published_date);
#     This MV hold all rows that may be updated.
#
#     Once the prepare_write_cmd part will be completed, rows in the view will be counted and saved.
#
#     When test is finished, rows will be counted in the view and validate that this count less then count before
#     running stress.
#

import logging
import re

from sdcm.sct_events import DataValidatorEvent, Severity
from sdcm.utils.common import get_profile_content

LOGGER = logging.getLogger(__name__)


# pylint: disable=too-many-instance-attributes
class LongevityDataValidator:
    SUFFIX_FOR_VIEW_AFTER_UPDATE = '_after_update'
    SUFFIX_EXPECTED_DATA_TABLE = '_expect'
    SUBSTRING_NOT_UPDATED = '_not_updated'
    SUBSTRING_DELETION = '_deletions'
    DEFAULT_FETCH_SIZE = 5000

    def __init__(self, longevity_self_object, user_profile_name, base_table_partition_keys,
                 stress_cmds_part='prepare_write_cmd'):
        """

        :param longevity_self_object: "self" object of longevity test that inherited from ClusterTester
        :param user_profile_name: name of user profile with appropriate defined views
        :param stress_cmds_part: name of stress part from test yaml
        :param base_table_partition_keys: used for test of updated rows. List of all primary keys of base table
                                          For example: ['domain', 'published_date']
        """
        self.longevity_self_object = longevity_self_object
        self.user_profile_name = user_profile_name
        self.stress_cmds_part = stress_cmds_part
        self.base_table_partition_keys = base_table_partition_keys

        self._validate_not_updated_data = True
        self._validate_updated_data = True
        self._keyspace_name = None
        self._mv_for_not_updated_data = None
        self._mvs_for_updated_data = None
        self._mvs_after_updated_data = None
        self._expected_data_table = None
        self._validate_not_updated_data = True
        self._validate_updated_data = True
        self._validate_updated_per_view = []
        self._mv_for_deletions = None
        self.rows_before_deletion = None

    @property
    def keyspace_name(self):
        if not self._keyspace_name:
            prepare_write_cmd = self.longevity_self_object.params.get(self.stress_cmds_part, default=[])
            profiles = [cmd for cmd in prepare_write_cmd if self.user_profile_name in cmd]
            if not profiles:
                self._validate_not_updated_data = False
                self._validate_updated_data = False

                LOGGER.warning('Keyspace is not recognized. '
                               'Data validation can\'t be performed')
                return self._keyspace_name

            cs_profile = profiles[0]
            _, profile = get_profile_content(cs_profile)
            self._keyspace_name = profile['keyspace']

        return self._keyspace_name

    @property
    def view_names_for_updated_data(self):
        """
        Get MV name that holds rows which are expected to be updated
        """
        if not self._mvs_for_updated_data:
            mvs_names = self.get_view_name_from_profile(self.SUFFIX_FOR_VIEW_AFTER_UPDATE, all_entries=True)
            self._mvs_for_updated_data = [view_name.replace(self.SUFFIX_FOR_VIEW_AFTER_UPDATE, '')
                                          for view_name in mvs_names]
            return self._mvs_for_updated_data

        return self._mvs_for_updated_data

    @property
    def view_names_after_updated_data(self):
        """
        Get MV name that holds rows which were updated
        """
        if not self._mvs_after_updated_data:
            self._mvs_after_updated_data = self.get_view_name_from_profile(self.SUFFIX_FOR_VIEW_AFTER_UPDATE,
                                                                           all_entries=True)
            return self._mvs_after_updated_data

        return self._mvs_after_updated_data

    @property
    def view_name_for_not_updated_data(self):
        """
        Get MV name that holds rows which are expected to stay intact
        """
        if not self._mv_for_not_updated_data:
            mv_for_not_updated_data = self.get_view_name_from_profile(self.SUBSTRING_NOT_UPDATED)

            self._mv_for_not_updated_data = mv_for_not_updated_data[0] if mv_for_not_updated_data else None
            return self._mv_for_not_updated_data

        return self._mv_for_not_updated_data

    @property
    def view_name_for_deletion_data(self):
        """
        Get MV name that holds rows which may be deleted
        """
        if not self._mv_for_deletions:
            mv_for_deletions = self.get_view_name_from_profile(self.SUBSTRING_DELETION)

            self._mv_for_deletions = mv_for_deletions[0] if mv_for_deletions else None
            return self._mv_for_deletions

        return self._mv_for_deletions

    @property
    def expected_data_table_name(self):
        """
        Table name for expected data (needs for validate rows which are expected to stay intact - implemented in
         validate_range_not_expected_to_change function)
        """
        if not self._expected_data_table and self.view_name_for_not_updated_data:
            self._expected_data_table = self.set_expected_data_table_name(self.view_name_for_not_updated_data)
        return self._expected_data_table

    def set_expected_data_table_name(self, view_name):
        return view_name + self.SUFFIX_EXPECTED_DATA_TABLE

    def get_view_name_from_profile(self, name_substr, cs_profile=None, all_entries=False):
        mv_names = []

        if not cs_profile:
            stress_cmd = self.longevity_self_object.params.get(self.stress_cmds_part, default=[])
            cs_profile = [cmd for cmd in stress_cmd if self.user_profile_name in cmd]

        if not cs_profile:
            return mv_names

        if not isinstance(cs_profile, list):
            cs_profile = [cs_profile]

        for profile in cs_profile:
            _, profile_content = get_profile_content(profile)
            mv_create_cmd = self.get_view_cmd_from_profile(profile_content, name_substr, all_entries)
            LOGGER.debug(f'Create commands: {mv_create_cmd}')
            for cmd in mv_create_cmd:
                view_name = self.get_view_name_from_stress_cmd(cmd, name_substr)
                if view_name:
                    mv_names.append(view_name)
                    if not all_entries:
                        break

        return mv_names

    @staticmethod
    def get_view_cmd_from_profile(profile_content, name_substr, all_entries=False):
        all_mvs = profile_content['extra_definitions']
        mv_cmd = [cmd for cmd in all_mvs if name_substr in cmd]

        mv_cmd = [mv_cmd[0]] if not all_entries and mv_cmd else mv_cmd
        return mv_cmd

    @staticmethod
    def get_view_name_from_stress_cmd(mv_create_cmd, name_substr):
        find_mv_name = re.search(r'materialized view (.*%s.*) as' % name_substr, mv_create_cmd, re.I)
        return find_mv_name.group(1) if find_mv_name else None

    def copy_immutable_expected_data(self):
        # Create expected data for immutable rows
        if self._validate_not_updated_data:
            if not (self.view_name_for_not_updated_data and self.expected_data_table_name):
                self._validate_not_updated_data = False
                DataValidatorEvent(type='warning', name='DataValidator', status=Severity.WARNING,
                                   message=f'Problem during copying expected data: view not found.'
                                   f'View name for not updated_data: {self.view_name_for_not_updated_data}; '
                                   f'Expected data table name {self.expected_data_table_name}. '
                                   f'Data validation of not updated rows won\' be performed')
                return

            LOGGER.debug(f'Copy expected data for immutable rows: {self.view_name_for_not_updated_data} -> '
                         f'{self.expected_data_table_name}')
            if not self.longevity_self_object.copy_view(node=self.longevity_self_object.db_cluster.nodes[0],
                                                        src_keyspace=self.keyspace_name,
                                                        src_view=self.view_name_for_not_updated_data,
                                                        dest_keyspace=self.keyspace_name,
                                                        dest_table=self.expected_data_table_name,
                                                        copy_data=True):
                self._validate_not_updated_data = False
                DataValidatorEvent(type='error', name='DataValidator', status=Severity.ERROR,
                                   error=f'Problem during copying expected data from {self.view_name_for_not_updated_data} '
                                         f'to {self.expected_data_table_name}. '
                                         f'Data validation of not updated rows won\' be performed')

    def copy_updated_expected_data(self):
        # Create expected data for updated rows
        if self._validate_updated_data:
            if not self.view_names_for_updated_data:
                self._validate_updated_per_view = [False]
                DataValidatorEvent(type='warning', name='DataValidator', status=Severity.WARNING,
                                   message=f'Problem during copying expected data: view not found. '
                                           f'View names for updated data: {self.view_names_for_updated_data}. '
                                           f'Data validation of updated rows won\' be performed')
                return

            LOGGER.debug(f'Copy expected data for updated rows. {self.view_names_for_updated_data}')
            for src_view in self.view_names_for_updated_data:
                expected_data_table_name = self.set_expected_data_table_name(src_view)
                LOGGER.debug(f'Expected data table name {expected_data_table_name}')
                if not self.longevity_self_object.copy_view(node=self.longevity_self_object.db_cluster.nodes[0],
                                                            src_keyspace=self.keyspace_name, src_view=src_view,
                                                            dest_keyspace=self.keyspace_name,
                                                            dest_table=expected_data_table_name,
                                                            columns_list=self.base_table_partition_keys,
                                                            copy_data=True):
                    self._validate_updated_per_view.append(False)
                    DataValidatorEvent(type='error', name='DataValidator', status=Severity.ERROR,
                                       error=f'Problem during copying expected data from {src_view} '
                                             f'to {expected_data_table_name}. '
                                             f'Data validation of updated rows won\' be performed')
                self._validate_updated_per_view.append(True)

    def save_count_rows_for_deletion(self):
        if not self.view_name_for_deletion_data:
            DataValidatorEvent(type='warning', name='DataValidator', status=Severity.WARNING,
                               message=f'Problem during copying expected data: not found. '
                               f'View name for deletion data: {self.view_name_for_deletion_data}. '
                               f'Data validation of deleted rows won\' be performed')
            return

        LOGGER.debug(f'Get rows count in {self.view_name_for_deletion_data} MV before stress')
        pk_name = self.base_table_partition_keys[0]
        with self.longevity_self_object.cql_connection_patient(self.longevity_self_object.db_cluster.nodes[0],
                                                               keyspace=self.keyspace_name) as session:
            rows_before_deletion = self.longevity_self_object.fetch_all_rows(session=session,
                                                                             default_fetch_size=self.DEFAULT_FETCH_SIZE,
                                                                             statement=f"SELECT {pk_name} FROM "
                                                                                       f"{self.view_name_for_deletion_data}")
            if rows_before_deletion:
                self.rows_before_deletion = len(rows_before_deletion)
                LOGGER.debug(f"{self.rows_before_deletion} rows for deletion")

    def validate_range_not_expected_to_change(self, session, during_nemesis=False):
        """
        Part of data in the user profile table shouldn't be updated using LWT.
        This data will be saved in the materialized view with "not_updated" substring in the name
        After prepare write all data from this materialized view will be saved in the separate table as expected result.
        During stress (after prepare) LWT update statements will be run for a few hours.
        When updates will be finished this function verifies that data in "not_updated" MV and expected result table
        is same
        """

        if not (self._validate_not_updated_data and self.view_name_for_not_updated_data
                and self.expected_data_table_name):
            LOGGER.debug('Verify immutable rows can\'t be performed as expected data has not been saved. '
                         'See error above in the sct.log')
            return

        if not during_nemesis:
            LOGGER.debug('Verify immutable rows')

        actual_result = self.longevity_self_object.fetch_all_rows(
            session=session, default_fetch_size=self.DEFAULT_FETCH_SIZE,
            statement=f"SELECT * FROM {self.view_name_for_not_updated_data}",
            verbose=not during_nemesis)
        if not actual_result:
            DataValidatorEvent(type='error', name='ImmutableRowsValidator', status=Severity.ERROR,
                               error=f"Can't validate immutable rows. "
                                     f"Fetch all rows from {self.view_name_for_not_updated_data} failed. "
                                     f"See error above in the sct.log")
            return

        expected_result = self.longevity_self_object.fetch_all_rows(
            session=session, default_fetch_size=self.DEFAULT_FETCH_SIZE,
            statement=f"SELECT * FROM {self.expected_data_table_name}",
            verbose=not during_nemesis)
        if not expected_result:
            DataValidatorEvent(type='error', name='ImmutableRowsValidator', status=Severity.ERROR,
                               error=f"Can't validate immutable rows. "
                                     f"Fetch all rows from {self.expected_data_table_name} failed. "
                                     f"See error above in the sct.log")
            return

        # Issue https://github.com/scylladb/scylla/issues/6181
        # Not fail the test if unexpected additional rows where found in actual result table
        if len(actual_result) > len(expected_result):
            DataValidatorEvent(type='warning', name='ImmutableRowsValidator', status=Severity.WARNING,
                               message=f'Actual dataset length {len(actual_result)} more '
                                       f'then expected dataset length: {len(expected_result)}. Issue #6181')
        else:
            if not during_nemesis:
                assert len(actual_result) == len(expected_result), \
                    'One or more rows are not as expected, suspected LWT wrong update. ' \
                    'Actual dataset length: {}, Expected dataset length: {}'.format(len(actual_result),
                                                                                    len(expected_result))

                assert (actual_result, expected_result,
                        'One or more rows are not as expected, suspected LWT wrong update')

                # raise info event in the end of test only
                DataValidatorEvent(type='info', name='ImmutableRowsValidator', status=Severity.NORMAL,
                                   message='Validation immutable rows finished successfully')
            else:
                if len(actual_result) < len(expected_result):
                    DataValidatorEvent(type='error', name='ImmutableRowsValidator', status=Severity.ERROR,
                                       error=f'Verify immutable rows. One or more rows didn\'t find as expected, '
                                       f'suspected LWT wrong update. '
                                       f'Actual dataset length: {len(actual_result)}, '
                                       f'Expected dataset length: {len(expected_result)}')
                else:
                    LOGGER.debug(
                        f'Verify immutable rows. '
                        f'Actual dataset length: {len(actual_result)}, Expected dataset length: {len(expected_result)}')

    def validate_range_expected_to_change(self, session, during_nemesis=False):
        """
        In user profile 'data_dir/c-s_lwt_basic.yaml' LWT updates the lwt_indicator and author columns with hard coded
        values.

        Two more materialized views are added. The first one holds rows that are candidates for the update
        (i.e. all rows before the update).
        The second one holds rows with lwt_indicator=30000000 (i.e. only the updated rows)

        After prepare all primay keys from first materialized view will be saved in the separate table as
        expected result.

        After the updates will be finished, 2 type of validation:
        1. All primary key values that saved in the expected result table, should be found in the both views
        2. Also validate row counts in the both veiws agains
        """
        if not (self._validate_updated_data and self.view_names_for_updated_data):
            LOGGER.debug('Verify updated rows can\'t be performed as expected data has not been saved. '
                         'See error above in the sct.log')
            return

        if not during_nemesis:
            LOGGER.debug('Verify updated rows')

        partition_keys = ', '.join(self.base_table_partition_keys)

        # List of tuples of correlated  view names for validation: before update, after update, expected data
        views_list = list(zip(self.view_names_for_updated_data,
                              self.view_names_after_updated_data,
                              [self.set_expected_data_table_name(view) for view in
                               self.view_names_for_updated_data],
                              self._validate_updated_per_view, ))
        for views_set in views_list:
            # views_set[0] - view name with rows before update
            # views_set[1] - view name with rows after update
            # views_set[2] - view name with all expected partition keys
            # views_set[3] - do perform validation for the view or not
            if not during_nemesis:
                LOGGER.debug(f'Verify updated row. View {views_set[0]}')
            if not views_set[3]:
                DataValidatorEvent(type='error', name='UpdatedRowsValidator', status=Severity.ERROR,
                                   error=f"Can't start validation for {views_set[0]}. "
                                         f"Copying expected data failed. See error above in the sct.log")
                return

            before_update_rows = self.longevity_self_object.fetch_all_rows(
                session=session, default_fetch_size=self.DEFAULT_FETCH_SIZE,
                statement=f"SELECT {partition_keys} FROM {views_set[0]}",
                verbose=not during_nemesis)
            if not before_update_rows:
                DataValidatorEvent(type='error', name='UpdatedRowsValidator', status=Severity.ERROR,
                                   error=f"Can't validate updated rows. "
                                         f"Fetch all rows from {views_set[0]} failed. See error above in the sct.log")
                return

            after_update_rows = self.longevity_self_object.fetch_all_rows(
                session=session, default_fetch_size=self.DEFAULT_FETCH_SIZE,
                statement=f"SELECT {partition_keys} FROM {views_set[1]}",
                verbose=not during_nemesis)
            if not after_update_rows:
                DataValidatorEvent(type='error', name='UpdatedRowsValidator', status=Severity.ERROR,
                                   error=f"Can't validate updated rows. "
                                         f"Fetch all rows from {views_set[1]} failed. See error above in the sct.log")
                return

            expected_rows = self.longevity_self_object.fetch_all_rows(
                session=session, default_fetch_size=self.DEFAULT_FETCH_SIZE,
                statement=f"SELECT {partition_keys} FROM {views_set[2]}",
                verbose=not during_nemesis)
            if not expected_rows:
                DataValidatorEvent(type='error', name='UpdatedRowsValidator', status=Severity.ERROR,
                                   error=f"Can't validate updated row. "
                                         f"Fetch all rows from {views_set[2]} failed. See error above in the sct.log")
                return

            # Issue https://github.com/scylladb/scylla/issues/6181
            # Not fail the test if unexpected additional rows where found in actual result table
            if len(before_update_rows) + len(after_update_rows) > len(expected_rows):
                DataValidatorEvent(type='warning', name='UpdatedRowsValidator', status=Severity.WARNING,
                                   message=f'View {views_set[0]}. '
                                           f'Actual dataset length {len(before_update_rows) + len(after_update_rows)} '
                                           f'more then expected dataset length: { len(expected_rows)}. Issue #6181')
            else:
                actual_data = sorted(before_update_rows+after_update_rows)
                expected_data = sorted(expected_rows)
                if not during_nemesis:
                    assert actual_data == expected_data,\
                        'One or more rows are not as expected, suspected LWT wrong update'

                    assert len(before_update_rows) + len(after_update_rows) == len(expected_rows), \
                        'One or more rows are not as expected, suspected LWT wrong update. '\
                        f'Actual dataset length: {len(before_update_rows) + len(after_update_rows)}, ' \
                        f'Expected dataset length: {len(expected_rows)}'

                    # raise info event in the end of test only
                    DataValidatorEvent(type='info', name='UpdatedRowsValidator', status=Severity.NORMAL,
                                       message=f'Validation updated rows finished successfully. View {views_set[0]}')
                else:
                    LOGGER.debug(f'Validation updated rows.  View {views_set[0]}. '
                                 f'Actual dataset length {len(before_update_rows) + len(after_update_rows)}, '
                                 f'Expected dataset length: {len(expected_rows)}.')

    def validate_deleted_rows(self, session, during_nemesis=False):
        """
        Part of data in the user profile table will be deleted using LWT.
        This data will be saved in the materialized view with "_deletions" substring in the name
        After prepare write rows count in this materialized view will be saved self.rows_before_deletion variable as
        expected result.
        During stress (after prepare) LWT delete statements will be run for a few hours.
        When stress will be finished this function verifies that rows count in "_deletions" MV will be less then it
        was saved in self.rows_before_deletion
        """
        if not self.rows_before_deletion:
            LOGGER.debug('Verify deleted rows can\'t be performed as expected rows count was had not been saved')
            return

        pk_name = self.base_table_partition_keys[0]
        if not during_nemesis:
            LOGGER.debug('Verify deleted rows')

        actual_result = self.longevity_self_object.fetch_all_rows(session=session, default_fetch_size=self.DEFAULT_FETCH_SIZE,
                                                                  statement=f"SELECT {pk_name} FROM {self.view_name_for_deletion_data}",
                                                                  verbose=not during_nemesis)
        if not actual_result:
            DataValidatorEvent(type='error', name='DeletedRowsValidator', status=Severity.ERROR,
                               error=f"Can't validate deleted rows. "
                                     f"Fetch all rows from {self.view_name_for_deletion_data} failed. "
                                     f"See error above in the sct.log")
            return

        if len(actual_result) < self.rows_before_deletion:
            if not during_nemesis:
                # raise info event in the end of test only
                DataValidatorEvent(type='info', name='DeletedRowsValidator', status=Severity.NORMAL,
                                   message='Validation deleted rows finished successfully')
            else:
                LOGGER.debug('Validation deleted rows finished successfully')
        else:
            LOGGER.warning('Deleted row were not found. May be issue #6181. '
                           'Actual dataset length: {}, Expected dataset length: {}'.format(len(actual_result),
                                                                                           self.rows_before_deletion))
