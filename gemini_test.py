#!/usr/bin/env python
import string
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
# Copyright (c) 2018 ScyllaDB

import time

import yaml
from cassandra import AlreadyExists, InvalidRequest

from sdcm.tester import ClusterTester


class GeminiTest(ClusterTester):

    """
    Test Scylla with gemini - tool for testing data integrity
    https://github.com/scylladb/gemini
    """
    gemini_results = {
        "cmd": ["N/A"],
        "status": "Not Running",
        "results": [],
        'errors': {}
    }

    def _pre_create_templated_user_schema(self, batch_start=None, batch_end=None, cs_user_profiles=None):
        # pylint: disable=too-many-locals
        user_profile_table_count = self.params.get(  # pylint: disable=invalid-name
            'user_profile_table_count') or 0
        cs_user_profiles = cs_user_profiles or self.params.get('cs_user_profiles')
        # read user-profile
        for profile_file in cs_user_profiles:
            with open(profile_file, encoding="utf-8") as fobj:
                profile_yaml = yaml.safe_load(fobj)
            keyspace_definition = profile_yaml['keyspace_definition']
            keyspace_name = profile_yaml['keyspace']
            table_template = string.Template(profile_yaml['table_definition'])

            with self.db_cluster.cql_connection_patient(node=self.db_cluster.nodes[0]) as session:
                # since we are using connection while nemesis is running (and we have more then 5000 tables in this
                # use case), we need a bigger timeout here to keep the following CQL commands from failing
                session.default_timeout = 60.0 * 5
                try:
                    session.execute(keyspace_definition)
                except AlreadyExists:
                    self.log.debug("keyspace [{}] exists".format(keyspace_name))
                self._pre_create_advanced_user_schema(profile_file=profile_file)

                if batch_start is not None and batch_end is not None:
                    table_range = range(batch_start, batch_end)
                else:
                    table_range = range(user_profile_table_count)
                self.log.debug('Pre Creating Schema for c-s with {} user tables'.format(user_profile_table_count))
                for i in table_range:
                    table_name = 'table{}'.format(i)
                    query = table_template.substitute(table_name=table_name)
                    try:
                        session.execute(query)
                    except AlreadyExists:
                        self.log.debug('table [{}] exists'.format(table_name))
                    self.log.debug('{} Created'.format(table_name))

                    for definition in profile_yaml.get('extra_definitions', []):
                        query = string.Template(definition).substitute(table_name=table_name)
                        try:
                            session.execute(query)
                        except (AlreadyExists, InvalidRequest) as exc:
                            self.log.debug('extra definition for [{}] exists [{}]'.format(table_name, str(exc)))

    def _pre_create_advanced_user_schema(self, profile_file: str):
        """
        Search a user-profile file.
        Look for a commented line containing "advanced_schema_file"
        If found - open the file found and run commands for its definitions, like UDT.
        """
        with open(profile_file, encoding="utf-8") as fobj:
            content = fobj.readlines()
        advanced_schema_file = [line.lstrip('#').strip() for line in content if
                                line.find('advanced_schema_file:') > 0]
        # Looking for a line of: # advanced_schema_file: scylla-qa-internal/custom_d1/advanced_schema_file.yaml
        if advanced_schema_file:
            advanced_schema_file = advanced_schema_file[0].split()[1]
        with self.db_cluster.cql_connection_patient(node=self.db_cluster.nodes[0]) as session:
            with open(advanced_schema_file, encoding="utf-8") as fobj:
                advanced_schema_yaml = yaml.safe_load(fobj)
            udt_definition = advanced_schema_yaml['udt_definition']
            for cql_cmd in udt_definition.strip().splitlines():
                try:
                    session.execute(cql_cmd)
                except AlreadyExists as error:
                    self.log.debug("Type already exists: %s", error)

    def test_random_load(self):
        """
        Run gemini tool
        """
        if prepare_cs_user_profiles := self.params.get('prepare_cs_user_profiles'):
            self._pre_create_templated_user_schema(cs_user_profiles=prepare_cs_user_profiles)
        cmd = self.params.get('gemini_cmd')

        self.log.debug('Start gemini benchmark')
        gemini_thread = self.run_gemini(cmd=cmd)
        self.gemini_results["cmd"] = gemini_thread.gemini_commands
        self.gemini_results.update(self.verify_gemini_results(queue=gemini_thread))

        self.verify_results()

    def test_load_random_with_nemesis(self):

        cmd = self.params.get('gemini_cmd')

        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(),
                                    tester_obj=self)

        self.log.debug('Start gemini benchmark')
        gemini_thread = self.run_gemini(cmd=cmd)
        self.gemini_results["cmd"] = gemini_thread.gemini_commands
        # sleep before run nemesis test_duration * .25
        sleep_before_start = float(self.params.get('test_duration')) * 60 * .1
        self.log.info('Sleep interval {}'.format(sleep_before_start))
        time.sleep(sleep_before_start)

        self.db_cluster.start_nemesis()

        self.gemini_results.update(self.verify_gemini_results(queue=gemini_thread))

        self.db_cluster.stop_nemesis(timeout=1600)

        self.verify_results()

    def test_load_random_with_nemesis_cdc_reader(self):
        cmd = self.params.get('gemini_cmd')
        cdc_stress = self.params.get('stress_cdclog_reader_cmd')
        update_es = self.params.get('store_cdclog_reader_stats_in_es')

        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(),
                                    tester_obj=self)

        self.log.debug('Start gemini benchmark')
        gemini_thread = self.run_gemini(cmd=cmd)
        self.gemini_results["cmd"] = gemini_thread.gemini_commands
        # wait gemini create schema and write some data
        self.db_cluster.wait_for_schema_agreement()
        cdc_stress_queue = self.run_cdclog_reader_thread(stress_cmd=cdc_stress,
                                                         keyspace_name="ks1",
                                                         base_table_name="table1")
        # sleep before run nemesis test_duration * .1
        sleep_before_start = float(self.params.get('test_duration')) * 60 * .1
        self.log.info('Sleep interval {}'.format(sleep_before_start))
        time.sleep(sleep_before_start)

        self.db_cluster.start_nemesis()

        self.gemini_results.update(self.verify_gemini_results(queue=gemini_thread))

        cdc_stress_results = self.verify_cdclog_reader_results(cdc_stress_queue, update_es)
        self.log.debug(cdc_stress_results)

        self.db_cluster.stop_nemesis(timeout=1600)

        self.verify_results()

    def test_gemini_and_cdc_reader(self):
        gemini_cmd = self.params.get('gemini_cmd')
        cdc_stress = self.params.get('stress_cdclog_reader_cmd')
        update_es = self.params.get('store_cdclog_reader_stats_in_es')

        gemini_thread = self.run_gemini(cmd=gemini_cmd)
        self.gemini_results["cmd"] = gemini_thread.gemini_commands

        # wait gemini create schema
        time.sleep(10)

        cdc_stress_queue = self.run_cdclog_reader_thread(stress_cmd=cdc_stress,
                                                         keyspace_name="ks1",
                                                         base_table_name="table1")

        self.gemini_results.update(self.verify_gemini_results(queue=gemini_thread))

        cdc_stress_results = self.verify_cdclog_reader_results(cdc_stress_queue, update_es)

        self.log.debug(cdc_stress_results)

        self.verify_results()

    def verify_results(self):
        if self.gemini_results['status'] == 'FAILED':
            self.fail(self.gemini_results['results'])

    def get_email_data(self):
        self.log.info('Prepare data for email')

        email_data = self._get_common_email_data()
        grafana_dataset = self.monitors.get_grafana_screenshot_and_snapshot(self.start_time) if self.monitors else {}

        if self.loaders:
            gemini_version = self.loaders.gemini_version
        else:
            self.log.warning("Failed to get gemini version as loader instance is not created")
            gemini_version = ""

        email_data.update({"gemini_cmd": self.gemini_results["cmd"],
                           "gemini_version": gemini_version,
                           "nemesis_details": self.get_nemesises_stats(),
                           "nemesis_name": self.params.get("nemesis_class_name"),
                           "number_of_oracle_nodes": self.params.get("n_test_oracle_db_nodes"),
                           "oracle_ami_id": self.params.get("ami_id_db_oracle"),
                           "oracle_db_version":
                               self.cs_db_cluster.nodes[0].scylla_version if self.cs_db_cluster else "N/A",
                           "oracle_instance_type": self.params.get("instance_type_db_oracle"),
                           "results": self.gemini_results["results"],
                           "scylla_ami_id": self.params.get("ami_id_db_scylla"),
                           "status": self.gemini_results["status"],
                           "grafana_screenshots": grafana_dataset.get("screenshots", []),
                           "grafana_snapshots": grafana_dataset.get("snapshots", [])})

        return email_data
