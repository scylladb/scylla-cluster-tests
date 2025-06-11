# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2020 ScyllaDB

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
import json
import os
import re
import logging
import uuid
from typing import NamedTuple, Optional

from sdcm.sct_events import Severity
from sdcm.test_config import TestConfig
from sdcm.utils.database_query_utils import fetch_all_rows

from sdcm.utils.user_profile import get_profile_content
from sdcm.sct_events.health import DataValidatorEvent


LOGGER = logging.getLogger(__name__)


class DataForValidation(NamedTuple):
    views: tuple  # list of view names with data for validation
    actual_data: list
    expected_data: list
    before_update_rows: Optional[list]
    after_update_rows: Optional[list]


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
        self.base_table_name = self.get_base_table_from_profile()

    @property
    def keyspace_name(self):
        if not self._keyspace_name:
            prepare_write_cmd = self.longevity_self_object.params.get(self.stress_cmds_part) or []
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
            LOGGER.debug("Keyspace name: %s, profile: %s", self._keyspace_name, profile)

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

    def get_profile_content(self, cs_profile=None):
        if not cs_profile:
            stress_cmd = self.longevity_self_object.params.get(self.stress_cmds_part) or []
            cs_profile = [cmd for cmd in stress_cmd if self.user_profile_name in cmd]

        if not cs_profile:
            return []

        if not isinstance(cs_profile, list):
            cs_profile = [cs_profile]

        return cs_profile

    def get_base_table_from_profile(self, cs_profile=None):
        for profile in self.get_profile_content(cs_profile):
            _, profile_content = get_profile_content(profile)
            if table_name := profile_content['table']:
                return table_name

        return None

    def get_view_name_from_profile(self, name_substr, cs_profile=None, all_entries=False):
        mv_names = []

        for profile in self.get_profile_content(cs_profile):
            _, profile_content = get_profile_content(profile)
            mv_create_cmd = self.get_view_cmd_from_profile(profile_content, name_substr, all_entries)
            LOGGER.debug('Create commands: %s', mv_create_cmd)
            for cmd in mv_create_cmd:
                view_name = self.get_view_name_from_stress_cmd(cmd, name_substr)
                if view_name:
                    mv_names.append(view_name)
                    if not all_entries:
                        break

        return list(dict.fromkeys(mv_names))  # list of unique view names and keep the elements order

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

    @staticmethod
    def get_entity_columns(entity_name: str, session):
        result = session.execute(f"SELECT * FROM {entity_name} LIMIT 1")
        return result.column_names

    def copy_immutable_expected_data(self):
        # Create expected data for immutable rows
        if self._validate_not_updated_data:
            if not (self.view_name_for_not_updated_data and self.expected_data_table_name):
                self._validate_not_updated_data = False
                DataValidatorEvent.DataValidator(
                    severity=Severity.WARNING,
                    message=f"Problem during copying expected data: view not found."
                    f"View name for not updated_data: {self.view_name_for_not_updated_data}; "
                    f"Expected data table name {self.expected_data_table_name}. "
                    f"Data validation of not updated rows won' be performed"
                ).publish()
                return

            LOGGER.debug('Copy expected data for immutable rows: %s -> %s',
                         self.view_name_for_not_updated_data, self.expected_data_table_name)
            if not self.longevity_self_object.copy_view(node=self.longevity_self_object.db_cluster.nodes[0],
                                                        src_keyspace=self.keyspace_name,
                                                        src_view=self.view_name_for_not_updated_data,
                                                        dest_keyspace=self.keyspace_name,
                                                        dest_table=self.expected_data_table_name,
                                                        copy_data=True):
                self._validate_not_updated_data = False
                DataValidatorEvent.DataValidator(
                    severity=Severity.ERROR,
                    error=f"Problem during copying expected data from {self.view_name_for_not_updated_data} "
                    f"to {self.expected_data_table_name}. "
                    f"Data validation of not updated rows won' be performed"
                ).publish()

    def copy_updated_expected_data(self):
        # Create expected data for updated rows
        if self._validate_updated_data:
            if not self.view_names_for_updated_data:
                self._validate_updated_per_view = [False]
                DataValidatorEvent.DataValidator(
                    severity=Severity.WARNING,
                    message=f"Problem during copying expected data: view not found. "
                    f"View names for updated data: {self.view_names_for_updated_data}. "
                    f"Data validation of updated rows won' be performed"
                ).publish()
                return

            LOGGER.debug('Copy expected data for updated rows. %s', self.view_names_for_updated_data)
            for src_view in self.view_names_for_updated_data:
                expected_data_table_name = self.set_expected_data_table_name(src_view)
                LOGGER.debug('Expected data table name %s', expected_data_table_name)
                if not self.longevity_self_object.copy_view(node=self.longevity_self_object.db_cluster.nodes[0],
                                                            src_keyspace=self.keyspace_name, src_view=src_view,
                                                            dest_keyspace=self.keyspace_name,
                                                            dest_table=expected_data_table_name,
                                                            columns_list=self.base_table_partition_keys,
                                                            copy_data=True):
                    self._validate_updated_per_view.append(False)
                    DataValidatorEvent.DataValidator(
                        severity=Severity.ERROR,
                        error=f"Problem during copying expected data from {src_view} to {expected_data_table_name}. "
                        f"Data validation of updated rows won' be performed"
                    ).publish()
                self._validate_updated_per_view.append(True)

    def save_count_rows_for_deletion(self):
        if not self.view_name_for_deletion_data:
            DataValidatorEvent.DataValidator(
                severity=Severity.WARNING,
                message=f"Problem during copying expected data: not found. "
                f"View name for deletion data: {self.view_name_for_deletion_data}. "
                f"Data validation of deleted rows won' be performed"
            ).publish()
            return

        LOGGER.debug('Get rows count in %s MV before stress', self.view_name_for_deletion_data)
        pk_name = self.base_table_partition_keys[0]
        with self.longevity_self_object.db_cluster.cql_connection_patient(
                self.longevity_self_object.db_cluster.nodes[0], keyspace=self.keyspace_name) as session:
            rows_before_deletion = fetch_all_rows(session=session, default_fetch_size=self.DEFAULT_FETCH_SIZE,
                                                  statement=f"SELECT {pk_name} FROM {self.view_name_for_deletion_data}")
            if rows_before_deletion:
                self.rows_before_deletion = len(rows_before_deletion)
                LOGGER.debug("%s rows for deletion", self.rows_before_deletion)

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

        actual_result = fetch_all_rows(session=session, default_fetch_size=self.DEFAULT_FETCH_SIZE,
                                       statement=f"SELECT * FROM {self.view_name_for_not_updated_data}",
                                       verbose=not during_nemesis)
        if not actual_result:
            DataValidatorEvent.ImmutableRowsValidator(
                severity=Severity.WARNING,
                message=f"Can't validate immutable rows. "
                f"Fetch all rows from {self.view_name_for_not_updated_data} failed. "
                f"See error above in the sct.log"
            ).publish()
            return

        expected_result = fetch_all_rows(session=session, default_fetch_size=self.DEFAULT_FETCH_SIZE,
                                         statement=f"SELECT * FROM {self.expected_data_table_name}",
                                         verbose=not during_nemesis)
        if not expected_result:
            DataValidatorEvent.ImmutableRowsValidator(
                severity=Severity.WARNING,
                message=f"Can't validate immutable rows. Fetch all rows from {self.expected_data_table_name} failed. "
                f"See error above in the sct.log"
            ).publish()
            return

        # Issue https://github.com/scylladb/scylla/issues/6181
        # Not fail the test if unexpected additional rows where found in actual result table
        if len(actual_result) > len(expected_result):
            DataValidatorEvent.ImmutableRowsValidator(
                severity=Severity.WARNING,
                message=f"Actual dataset length more then expected ({len(actual_result)} > {len(expected_result)}). "
                f"Issue #6181"
            ).publish()
        elif not during_nemesis:
            assert len(actual_result) == len(expected_result), \
                'One or more rows are not as expected, suspected LWT wrong update. ' \
                'Actual dataset length: {}, Expected dataset length: {}'.format(len(actual_result),
                                                                                len(expected_result))

            assert actual_result == expected_result, \
                'One or more rows are not as expected, suspected LWT wrong update'

            # Raise info event at the end of the test only.
            DataValidatorEvent.ImmutableRowsValidator(
                severity=Severity.NORMAL,
                message="Validation immutable rows finished successfully"
            ).publish()
        elif len(actual_result) < len(expected_result):
            DataValidatorEvent.ImmutableRowsValidator(
                severity=Severity.ERROR,
                error=f"Verify immutable rows. "
                f"One or more rows not found as expected, suspected LWT wrong update. "
                f"Actual dataset length: {len(actual_result)}, "
                f"Expected dataset length: {len(expected_result)}"
            ).publish()
        else:
            LOGGER.debug('Verify immutable rows. Actual dataset length: %s, Expected dataset length: %s',
                         len(actual_result), len(expected_result))

    def list_of_view_names_for_update_test(self):
        # List of tuples of correlated  view names for validation: before update, after update, expected data
        return list(zip(self.view_names_for_updated_data,
                        self.view_names_after_updated_data,
                        [self.set_expected_data_table_name(view) for view in
                         self.view_names_for_updated_data],
                        self._validate_updated_per_view, ))

    def fetch_data_for_validation_after_update(self, during_nemesis: bool, views_set: tuple, session) -> \
            Optional[DataForValidation]:
        # views_set[0] - view name with rows before update
        # views_set[1] - view name with rows after update
        # views_set[2] - view name with all expected partition keys
        # views_set[3] - do perform validation for the view or not
        partition_keys = ', '.join(self.base_table_partition_keys)

        before_update_rows = fetch_all_rows(session=session, default_fetch_size=self.DEFAULT_FETCH_SIZE,
                                            statement=f"SELECT {partition_keys} FROM {views_set[0]}",
                                            verbose=not during_nemesis)
        if not before_update_rows:
            DataValidatorEvent.UpdatedRowsValidator(
                severity=Severity.WARNING,
                message=f"Can't validate updated rows. Fetch all rows from {views_set[0]} failed. "
                f"See error above in the sct.log"
            ).publish()
            return None

        after_update_rows = fetch_all_rows(session=session, default_fetch_size=self.DEFAULT_FETCH_SIZE,
                                           statement=f"SELECT {partition_keys} FROM {views_set[1]}",
                                           verbose=not during_nemesis)
        if not after_update_rows:
            DataValidatorEvent.UpdatedRowsValidator(
                severity=Severity.WARNING,
                message=f"Can't validate updated rows. Fetch all rows from {views_set[1]} failed. "
                f"See error above in the sct.log"
            ).publish()
            return None

        expected_rows = fetch_all_rows(session=session, default_fetch_size=self.DEFAULT_FETCH_SIZE,
                                       statement=f"SELECT {partition_keys} FROM {views_set[2]}",
                                       verbose=not during_nemesis)
        if not expected_rows:
            DataValidatorEvent.UpdatedRowsValidator(
                severity=Severity.WARNING,
                message=f"Can't validate updated row. Fetch all rows from {views_set[2]} failed. "
                f"See error above in the sct.log"
            ).publish()
            return None

        return DataForValidation(views=views_set,
                                 actual_data=sorted(before_update_rows + after_update_rows),
                                 expected_data=sorted(expected_rows),
                                 before_update_rows=before_update_rows,
                                 after_update_rows=after_update_rows)

    @staticmethod
    def save_data_for_debugging(data_for_validation: DataForValidation) -> str:
        log_dir = TestConfig().logdir()
        logdir = os.path.join(log_dir, "lwt_validator_data_for_debug")
        os.makedirs(logdir, exist_ok=True)
        unique_index = uuid.UUID

        with open(os.path.join(logdir, f"{data_for_validation.views[0]}_{unique_index}_debug.json"), "w",
                  encoding='utf8') as json_file:
            json.dump(sorted(data_for_validation.before_update_rows), json_file)
            LOGGER.info("before_update_rows json: %s", json_file.name)

        with open(os.path.join(logdir, f"{data_for_validation.views[1]}_{unique_index}_debug.json"), "w",
                  encoding='utf8') as json_file:
            json.dump(sorted(data_for_validation.after_update_rows), json_file)
            LOGGER.info("after_update_rows json: %s", json_file.name)

        with open(os.path.join(logdir, f"{data_for_validation.views[2]}_{unique_index}_debug.json"), "w",
                  encoding='utf8') as json_file:
            json.dump(sorted(data_for_validation.expected_data), json_file)
            LOGGER.info("expected_rows json: %s", json_file.name)

        with open(os.path.join(logdir, f"{data_for_validation.views[0]}_{unique_index}_actual_data_debug.json"), "w",
                  encoding='utf8') as json_file:
            json.dump(sorted(data_for_validation.actual_data), json_file)
            LOGGER.info("actual_data json: %s", json_file.name)

        return logdir

    def analyze_updated_data_and_save_in_file(self, data_for_validation: DataForValidation, session, logdir: str):
        actual_data_set = {tuple(item) for item in sorted(data_for_validation.actual_data)}
        expected_data_set = {tuple(item) for item in sorted(data_for_validation.expected_data)}
        difference_set = expected_data_set - actual_data_set  # missed in the actual data after update

        if not self.base_table_name:
            DataValidatorEvent.UpdatedRowsValidator(
                severity=Severity.ERROR,
                error="Analyzing of updated rows failed. Failed to get base table name from profile").publish()
            return

        result = {}
        select_pk_columns = ', '.join(self.base_table_partition_keys)
        for i, diff_row in enumerate(difference_set):
            pks_for_missed_row = {}
            missed_row_as_str = []
            for pk_index, pk_name in enumerate(self.base_table_partition_keys):
                pks_for_missed_row[pk_name] = diff_row[pk_index]
                missed_row_as_str.append(f"{pk_name} = {diff_row[pk_index]}")

            result[i] = {'row': ", ".join(missed_row_as_str)}
            # data_for_validation.views[0] - view name with rows before update
            # data_for_validation.views[1] - view name with rows after update
            # data_for_validation.views[2] - view name with all expected partition keys
            columns_for_validation = ', '.join(self.get_entity_columns(entity_name=data_for_validation.views[0],
                                                                       session=session))
            query = f"select %s f" \
                f"rom %s where {' and '.join(missed_row_as_str)}"

            row_data = {}
            for table in data_for_validation.views[:-1]:
                if 'after_update' in table:
                    key = 'after'
                    select_columns = columns_for_validation
                elif 'actual_data' in table:
                    key = 'actual'
                    select_columns = columns_for_validation
                elif 'expect' in table:
                    key = 'expect'
                    select_columns = select_pk_columns
                else:
                    key = 'before'
                    select_columns = columns_for_validation

                try:
                    row_data.update({key: list(session.execute(query % (select_columns, table)))})
                except Exception as error:  # noqa: BLE001
                    LOGGER.error("Query %s failed. Error: %s", query % table, error)

            row_data.update({'source_all_columns': list(session.execute(query % (columns_for_validation,
                                                                                 self.base_table_name)))})

            if row_data['before'] or row_data['after']:
                message = (f"Row {diff_row} is found in the "
                           f"{data_for_validation.views[0] if row_data['before'] else data_for_validation.views[1]} "
                           f"view despite was not fetched. Test problem, failure in the fetcher\n"
                           f"Row data in the source table: {row_data['source_all_columns']}\n"
                           f"Row data in the expected table: {row_data['expect']}")
                LOGGER.debug(message)

            else:
                error = (f"Row {diff_row} is expected to be find in whether before (the row was not updated) or "
                         f"after (the row was updated) update view, but was not found.\n"
                         f"Possible reason of the problem: "
                         f"the row was updated but with not expected value as it defined in profile.\n"
                         f"Row data in the source table: {row_data['source_all_columns']}")
                LOGGER.error(error)
                result[i].update({'rows': row_data})

        if result:
            with open(os.path.join(logdir, f"{data_for_validation.views[0]}_analyze_result_debug.json"), "w",
                      encoding='utf8') as json_file:
                try:
                    json.dump(result, json_file)
                except:
                    json_file.write(str(result))
                LOGGER.info("analyze_result_debug json: %s", json_file.name)

            # stop test
            DataValidatorEvent.UpdatedRowsValidator(
                severity=Severity.CRITICAL,
                error=f"Validation updated rows failed. View {data_for_validation.views[0]}."
                "Failed data has been analyzed and saved with json format. "
                "Find the result files in sct-runner archive").publish()

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

        # List of tuples of correlated  view names for validation: before update, after update, expected data
        views_list = self.list_of_view_names_for_update_test()
        for views_set in views_list:
            # # views_set[0] - view name with rows before update
            # # views_set[1] - view name with rows after update
            # # views_set[2] - view name with all expected partition keys
            # # views_set[3] - do perform validation for the view or not
            if not during_nemesis:
                LOGGER.debug('Verify updated row. View %s', views_set[0])

            if not views_set[3]:
                DataValidatorEvent.UpdatedRowsValidator(
                    severity=Severity.WARNING,
                    message=f"Can't start validation for {views_set[0]}. Copying expected data failed. "
                    f"See error above in the sct.log"
                ).publish()
                continue

            data_for_validation = self.fetch_data_for_validation_after_update(during_nemesis=during_nemesis,
                                                                              views_set=views_set,
                                                                              session=session)
            if data_for_validation is None:
                continue

            # Issue https://github.com/scylladb/scylla/issues/6181
            # Not fail the test if unexpected additional rows where found in actual result table
            if len(data_for_validation.actual_data) > len(data_for_validation.expected_data):
                DataValidatorEvent.UpdatedRowsValidator(
                    severity=Severity.WARNING,
                    message=f"View {views_set[0]}. "
                    f"Actual dataset length {len(data_for_validation.actual_data)} "
                    f"more then expected dataset length: {len(data_for_validation.expected_data)}. "
                    f"Issue #6181"
                ).publish()
                continue

            if during_nemesis:
                LOGGER.debug('Validation updated rows.  View %s. Actual dataset length %s, '
                             'Expected dataset length: %s.',
                             data_for_validation.views[0], len(data_for_validation.actual_data),
                             len(data_for_validation.expected_data))
                continue

            if (len(data_for_validation.actual_data) != len(data_for_validation.expected_data)) \
                    or data_for_validation.actual_data != data_for_validation.expected_data:
                LOGGER.debug("%s. Rows amount:\n  before update: %s\n  after update: %s\n  expected: %s\n "
                             "actual: %s",
                             data_for_validation.views[0], len(data_for_validation.before_update_rows),
                             len(data_for_validation.after_update_rows), len(data_for_validation.expected_data),
                             len(data_for_validation.actual_data))

                logdir = self.save_data_for_debugging(data_for_validation)

                self.analyze_updated_data_and_save_in_file(data_for_validation=data_for_validation,
                                                           session=session,
                                                           logdir=logdir)
            else:
                DataValidatorEvent.UpdatedRowsValidator(
                    severity=Severity.NORMAL,
                    message=f"Validation updated rows finished successfully. View {data_for_validation.views[0]}"
                ).publish()

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
            LOGGER.debug('Verify deleted rows can\'t be performed as expected rows count had not been saved')
            return

        pk_name = self.base_table_partition_keys[0]
        if not during_nemesis:
            LOGGER.debug('Verify deleted rows')

        actual_result = fetch_all_rows(session=session, default_fetch_size=self.DEFAULT_FETCH_SIZE,
                                       statement=f"SELECT {pk_name} FROM "
                                       f"{self.view_name_for_deletion_data}", verbose=not during_nemesis)
        if actual_result is None:
            DataValidatorEvent.DeletedRowsValidator(
                severity=Severity.ERROR,
                error=f"Can't validate deleted rows. Fetch all rows from {self.view_name_for_deletion_data} failed. "
                f"See error above in the sct.log"
            ).publish()
            return

        if len(actual_result) < self.rows_before_deletion:
            if not during_nemesis:
                # raise info event in the end of test only
                DataValidatorEvent.DeletedRowsValidator(
                    severity=Severity.NORMAL,
                    message="Validation deleted rows finished successfully"
                ).publish()
            else:
                LOGGER.debug('Validation deleted rows finished successfully')
        elif len(actual_result) == self.rows_before_deletion:
            DataValidatorEvent.DeletedRowsValidator(
                severity=Severity.WARNING,
                message="Rows were not deleted. Maybe need to increase dataset for delete."
            ).publish()
        else:
            LOGGER.warning('Deleted row were not found. May be issue #6181. '
                           'Actual dataset length: {}, Expected dataset length: {}'.format(len(actual_result),
                                                                                           self.rows_before_deletion))
