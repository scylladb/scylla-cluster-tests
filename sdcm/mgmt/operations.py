import re
import time
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from pathlib import Path
from textwrap import dedent

import boto3
import yaml
from invoke import exceptions

from sdcm.mgmt import TaskStatus
from sdcm.mgmt.cli import ScyllaManagerTool
from sdcm import mgmt
from sdcm.exceptions import FilesNotCorrupted
from sdcm.remote import shell_script_cmd, LOCALRUNNER
from sdcm.sct_events.system import InfoEvent
from sdcm.test_config import TestConfig
from sdcm.tester import ClusterTester
from sdcm.utils.azure_utils import AzureService
from sdcm.utils.cluster_tools import flush_nodes, major_compaction_nodes, clear_snapshot_nodes
from sdcm.utils.compaction_ops import CompactionOps
from sdcm.utils.gce_utils import get_gce_storage_client
from sdcm.utils.loader_utils import LoaderUtilsMixin
from sdcm.utils.time_utils import ExecutionTimer
from sdcm.utils.version_utils import ComparableScyllaVersion


class ClusterOperations(ClusterTester):
    CLUSTER_NAME = "mgr_cluster1"

    def ensure_and_get_cluster(self, manager_tool, force_add: bool = False, **add_cluster_extra_params):
        """Get the cluster if it is already added, otherwise add it to manager.

        Use force_add=True if you want to re-add the cluster (delete and add again) even if it already added.
        Use add_cluster_extra_params to pass additional parameters (like 'client_encrypt') to the add_cluster method.
        """
        mgr_cluster = manager_tool.get_cluster(cluster_name=self.CLUSTER_NAME)

        if not mgr_cluster or force_add:
            if mgr_cluster:
                mgr_cluster.delete()

            add_cluster_params = {}
            add_cluster_params.update(
                name=self.CLUSTER_NAME,
                db_cluster=self.db_cluster,
                auth_token=self.monitors.mgmt_auth_token,
                force_non_ssl_session_port=manager_tool.is_force_non_ssl_session_port(db_cluster=self.db_cluster),
            )
            add_cluster_params.update(add_cluster_extra_params)

            mgr_cluster = manager_tool.add_cluster(**add_cluster_params)

        return mgr_cluster

    def get_cluster_hosts_ip(self):
        return ScyllaManagerTool.get_cluster_hosts_ip(self.db_cluster)

    def get_cluster_hosts_with_ips(self):
        return ScyllaManagerTool.get_cluster_hosts_with_ips(self.db_cluster)

    def get_all_dcs_names(self):
        dcs_names = set()
        for node in self.db_cluster.nodes:
            data_center = self.db_cluster.get_nodetool_info(node)['Data Center']
            dcs_names.add(data_center)
        return dcs_names

    def get_rf_based_on_nodes_number(self) -> dict[str, int]:
        """Define replication factor based on a number of nodes per DC(s).

        Note: replication factor per DC will be equal to the number of nodes in that DC.
              Adjust the method if you need a custom value to be put there.

        Example of return value:
            {
                "eu-west-2scylla_node_west": 2,
                "us-eastscylla_node_east": 1
            }
        """
        nodetool_status = self.db_cluster.get_nodetool_status(self.db_cluster.nodes[0])
        rf = {dc_name: len(nodes) for dc_name, nodes in nodetool_status.items()}
        return rf

    def disable_compaction(self):
        compaction_ops = CompactionOps(cluster=self.db_cluster)
        for node in self.db_cluster.nodes:
            compaction_ops.disable_autocompaction_on_ks_cf(node=node)

    def _cluster_flush_and_major_compaction(self, keyspace: str, table: str):
        InfoEvent(message='Flush cluster then run a major compaction and wait for finish').publish()
        flush_nodes(cluster=self.db_cluster, keyspace=keyspace)
        major_compaction_nodes(cluster=self.db_cluster, keyspace=keyspace, table=table)
        self.wait_no_compactions_running(n=400, sleep_time=60)

    def align_cluster_data_state(self, keyspace: str, table: str, clear_snapshots: bool = True):
        if clear_snapshots:
            clear_snapshot_nodes(cluster=self.db_cluster)
        self._cluster_flush_and_major_compaction(keyspace, table)
        self.run_fstrim_on_all_db_nodes()


class BucketOperations(ClusterTester):
    backup_azure_blob_service = None
    backup_azure_blob_sas = None

    def _run_cmd_with_retry(self, executor, cmd, retries=10):
        for _ in range(retries):
            try:
                executor(cmd)
                break
            except exceptions.UnexpectedExit as ex:
                self.log.debug("cmd %s failed with error %s, will retry", cmd, ex)

    def install_awscli_dependencies(self, node):
        if node.distro.is_ubuntu or node.distro.is_debian:
            cmd = dedent("""
                apt update
                apt install -y python3-pip
                PIP_BREAK_SYSTEM_PACKAGES=1 pip install awscli
            """)
        elif node.distro.is_rhel_like:
            cmd = dedent("""
            yum install -y epel-release
            yum install -y python-pip
            yum remove -y epel-release
            pip install awscli==1.18.140
            """)
        else:
            raise RuntimeError("This distro is not supported")
        self._run_cmd_with_retry(executor=node.remoter.sudo, cmd=shell_script_cmd(cmd))

    def install_gsutil_dependencies(self, node):
        if node.distro.is_ubuntu or node.distro.is_debian:
            cmd = dedent("""
                echo 'deb https://packages.cloud.google.com/apt cloud-sdk main' | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
                curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
                sudo apt-get update
            """)
            self._run_cmd_with_retry(executor=node.remoter.run, cmd=shell_script_cmd(cmd))
            node.install_package("google-cloud-cli")
        else:
            raise NotImplementedError("At the moment, we only support debian installation")

    def install_azcopy_dependencies(self, node):
        self._run_cmd_with_retry(executor=node.remoter.sudo, cmd=shell_script_cmd("""\
            curl -L https://aka.ms/downloadazcopy-v10-linux | \
                tar xz -C /usr/bin --strip-components 1 --wildcards '*/azcopy'
        """))
        self.backup_azure_blob_service = \
            f"https://{self.test_config.backup_azure_blob_credentials['account']}.blob.core.windows.net/"
        self.backup_azure_blob_sas = self.test_config.backup_azure_blob_credentials["download_sas"]

    @staticmethod
    def download_from_s3(node, source, destination):
        node.remoter.sudo(f"aws s3 cp '{source}' '{destination}'")

    @staticmethod
    def download_from_gs(node, source, destination):
        node.remoter.sudo(f"gsutil cp '{source.replace('gcs://', 'gs://')}' '{destination}'")

    def download_from_azure(self, node, source, destination):
        # azure://<bucket>/<path> -> https://<account>.blob.core.windows.net/<bucket>/<path>?SAS
        source = f"{source.replace('azure://', self.backup_azure_blob_service)}{self.backup_azure_blob_sas}"
        node.remoter.sudo(f"azcopy copy '{source}' '{destination}'")

    @staticmethod
    def create_s3_bucket(name: str, region: str) -> None:
        LOCALRUNNER.run(f"aws s3 mb s3://{name} --region {region}")

    @staticmethod
    def create_gs_bucket(name: str, region: str) -> None:
        LOCALRUNNER.run(f"gsutil mb -l {region} gs://{name}")

    @staticmethod
    def sync_s3_buckets(source: str, destination: str, acl: str = 'bucket-owner-full-control') -> None:
        LOCALRUNNER.run(f"aws s3 sync s3://{source} s3://{destination} --acl {acl}")

    @staticmethod
    def sync_gs_buckets(source: str, destination: str) -> None:
        LOCALRUNNER.run(f"gsutil -m rsync -r gs://{source} gs://{destination}")

    def get_region_from_bucket_location(self, location: str) -> str:
        cluster_backend = self.params.get("cluster_backend")

        if cluster_backend == "aws":
            # extract region name from AWS_US_EAST_1:s3:scylla-cloud-backup-8072-7216-v5dn53 to us-east-1
            return location.split(":")[0].split("_", 1)[1].replace("_", "-").lower()
        elif cluster_backend == "gce":
            # extract region name from GCE_US_EAST_1:gcs:scylla-cloud-backup-23-29-6q0i5q to us-east1
            parts = location.split(":")[0].split("_")[1:]
            assert len(parts) == 3, f"Can't extract region from location {location}"
            return f"{parts[0].lower()}-{parts[1].lower()}{parts[2]}"
        else:
            raise ValueError(f"Unsupported cluster backend - {cluster_backend}, should be either aws or gce")

    def copy_backup_snapshot_bucket(self, source: str, destination: str, region: str) -> None:
        """Copy bucket with Manager backup snapshots.
        The process consists of two stages - new bucket creation and data sync (original bucket -> newly created).
        The main use case is to make a copy of a bucket created in a test with Cloud (siren) cluster since siren
        deletes the bucket together with cluster. Thus, if there is a goal to reuse backup snapshot of such cluster
        afterward, it should be copied to a new bucket.

        Only AWS and GCE backends are supported.
        """
        cluster_backend = self.params.get("cluster_backend")
        region = next(iter(self.params.region_names), '')

        if cluster_backend == "aws":
            self.create_s3_bucket(name=destination, region=region)
            self.sync_s3_buckets(source=source, destination=destination)
        elif cluster_backend == "gce":
            self.create_gs_bucket(name=destination, region=region)
            self.sync_gs_buckets(source=source, destination=destination)
        else:
            raise ValueError(f"Unsupported cluster backend - {cluster_backend}, should be either aws or gce")


@dataclass
class SnapshotData:
    """Describes the backup snapshot:

    - locations: backup locations
    - tag: snapshot tag, for example, 'sm_20240816185129UTC'
    - exp_timeout: expected timeout for the restore operation
    - dataset: dict with snapshot dataset details such as cl, replication, schema, etc.
    - keyspaces: list of keyspaces presented in backup
    - cs_read_cmd_template: cassandra-stress read command template
    - prohibit_verification_read: if True, the verification read will be prohibited. Most likely, such a backup was
      created via c-s user profile.
    - node_ids: list of node ids where backup was created
    - one_one_restore_params: dict with parameters for 1-1 restore ('account_credential_id' and 'sm_cluster_id')
    """
    locations: list[str]
    tag: str
    exp_timeout: int
    dataset: dict[str, str | int | dict]
    keyspaces: list[str]
    ks_tables_map: dict[str, list[str]]
    cs_read_cmd_template: str
    prohibit_verification_read: bool
    node_ids: list[str]
    one_one_restore_params: dict[str, str | int] | None


class SnapshotOperations(ClusterTester):

    @staticmethod
    def get_snapshot_data(snapshot_name: str) -> SnapshotData:
        snapshots_config = "defaults/manager_restore_benchmark_snapshots.yaml"
        with open(snapshots_config, encoding="utf-8") as snapshots_yaml:
            all_snapshots_dict = yaml.safe_load(snapshots_yaml)

        try:
            snapshot_dict = all_snapshots_dict["sizes"][snapshot_name]
        except KeyError:
            raise ValueError(f"Snapshot data for '{snapshot_name}' was not found in the {snapshots_config} file")

        ks_tables_map = {}
        for ks, ts in snapshot_dict["dataset"]["schema"].items():
            t_names = [list(t.keys())[0] for t in ts]
            ks_tables_map[ks] = t_names

        snapshot_data = SnapshotData(
            locations=snapshot_dict["locations"],
            tag=snapshot_dict["tag"],
            exp_timeout=snapshot_dict["exp_timeout"],
            dataset=snapshot_dict["dataset"],
            keyspaces=list(snapshot_dict["dataset"]["schema"].keys()),
            ks_tables_map=ks_tables_map,
            cs_read_cmd_template=all_snapshots_dict["cs_read_cmd_template"],
            prohibit_verification_read=snapshot_dict["prohibit_verification_read"],
            node_ids=snapshot_dict.get("node_ids"),
            one_one_restore_params=snapshot_dict.get("one_one_restore_params"),
        )
        return snapshot_data

    @staticmethod
    def _get_all_snapshot_files_s3(cluster_id, bucket_name, region_name):
        file_set = set()
        s3_client = boto3.client('s3', region_name=region_name)
        paginator = s3_client.get_paginator('list_objects')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=f'backup/sst/cluster/{cluster_id}')
        for page in pages:
            # No Contents key means that no snapshot file of the cluster exist,
            # probably no backup ran before this function
            if "Contents" in page:
                content_list = page["Contents"]
                file_set.update([item["Key"] for item in content_list])
        return file_set

    @staticmethod
    def _get_all_snapshot_files_gce(cluster_id, bucket_name):
        file_set = set()
        storage_client, _ = get_gce_storage_client()
        blobs = storage_client.list_blobs(bucket_or_name=bucket_name, prefix=f'backup/sst/cluster/{cluster_id}')
        for listing_object in blobs:
            file_set.add(listing_object.name)
        # Unlike S3, if no files match the prefix, no error will occur
        return file_set

    @staticmethod
    def _get_all_snapshot_files_azure(cluster_id, bucket_name):
        file_set = set()
        azure_service = AzureService()
        container_client = azure_service.blob.get_container_client(container=bucket_name)
        dir_listing = container_client.list_blobs(name_starts_with=f'backup/sst/cluster/{cluster_id}')
        for listing_object in dir_listing:
            file_set.add(listing_object.name)
        return file_set

    def get_all_snapshot_files(self, cluster_id):
        region_name = next(iter(self.params.region_names), '')
        bucket_name = self.params.get('backup_bucket_location').split()[0].format(region=region_name)
        if self.params.get('backup_bucket_backend') == 's3':
            return self._get_all_snapshot_files_s3(cluster_id=cluster_id, bucket_name=bucket_name,
                                                   region_name=region_name)
        elif self.params.get('backup_bucket_backend') == 'gcs':
            return self._get_all_snapshot_files_gce(cluster_id=cluster_id, bucket_name=bucket_name)
        elif self.params.get('backup_bucket_backend') == 'azure':
            return self._get_all_snapshot_files_azure(cluster_id=cluster_id, bucket_name=bucket_name)
        else:
            raise ValueError(f'"{self.params.get("backup_bucket_backend")}" not supported')


class SnapshotPreparerOperations(ClusterTester):
    ks_name_template = "{size}gb_{compaction}_{cl}_{col_size}_{col_n}_{scylla_version}"

    @staticmethod
    def _abbreviate_compaction_strategy_name(compaction_strategy: str) -> str:
        """Abbreviate and lower compaction strategy name which comes from c-s cmd to make it more readable in ks name.

        For example, LeveledCompactionStrategy -> lcs or SizeTieredCompactionStrategy -> stcs.
        """
        return ''.join(char for char in compaction_strategy if char.isupper()).lower()

    def _build_ks_name(self, backup_size: int, cs_cmd_params: dict) -> str:
        """Build the keyspace name based on the backup size and the parameters used in the c-s command.
        The name should include all the parameters important for c-s read verification and can be used to
        recreate such a command based on ks_name.
        """
        scylla_version = re.sub(r"[.]", "_", self.db_cluster.nodes[0].scylla_version)
        ks_name = self.ks_name_template.format(
            size=backup_size,
            compaction=self._abbreviate_compaction_strategy_name(cs_cmd_params.get("compaction")),
            cl=cs_cmd_params.get("cl").lower(),
            col_size=cs_cmd_params.get("col_size"),
            col_n=cs_cmd_params.get("col_n"),
            scylla_version=scylla_version,
        )
        return ks_name.replace("~", "_")

    def calculate_rows_per_loader(self, overall_rows_num: int) -> int:
        """Calculate number of rows per loader thread based on the overall number of rows and the number of loaders."""
        num_of_loaders = int(self.params.get("n_loaders"))
        if overall_rows_num % num_of_loaders:
            raise ValueError(f"Overall rows number ({overall_rows_num}) should be divisible by the number of loaders")
        return int(overall_rows_num / num_of_loaders)

    def build_snapshot_preparer_cs_write_cmd(self, backup_size: int) -> tuple[str, list[str]]:
        """Build the c-s command from 'mgmt_snapshots_preparer_params' parameters based on backup size.

        Extra params complete the missing part of command template.
        Among them are keyspace_name, num_of_rows, sequence_start and sequence_end.

        Returns:
            - ks_name: keyspace name
            - cs_cmds: list of c-s commands to be executed
        """
        overall_num_of_rows = backup_size * 1024 * 1024  # Considering 1 row = 1Kb
        rows_per_loader = self.calculate_rows_per_loader(overall_num_of_rows)

        preparer_params = self.params.get("mgmt_snapshots_preparer_params")

        cs_cmd_template = preparer_params.get("cs_cmd_template")
        cs_cmd_params = {key: value for key, value in preparer_params.items() if key != "cs_cmd_template"}
        extra_params = {"num_of_rows": rows_per_loader}

        params_to_use_in_cs_cmd = {**cs_cmd_params, **extra_params}

        ks_name = self._build_ks_name(backup_size, params_to_use_in_cs_cmd)
        params_to_use_in_cs_cmd["ks_name"] = ks_name

        cs_cmds = []
        num_of_loaders = int(self.params.get("n_loaders"))
        for loader_index in range(num_of_loaders):
            # Sequence params should be defined for every loader thread separately since vary for each thread
            params_to_use_in_cs_cmd["sequence_start"] = rows_per_loader * loader_index + 1
            params_to_use_in_cs_cmd["sequence_end"] = rows_per_loader * (loader_index + 1)

            cs_cmd = cs_cmd_template.format(**params_to_use_in_cs_cmd)
            cs_cmds.append(cs_cmd)

        return ks_name, cs_cmds

    def build_cs_read_cmd_from_snapshot_details(self, snapshot: SnapshotData) -> list[str]:
        """Define a list of cassandra-stress read commands from snapshot (dataset) details.

        C-S read command template and snapshot details are defined in defaults/manager_restore_benchmark_snapshots.yaml.
        Number of commands is equal to the number of loaders defined in the test parameters.
        """
        dataset = snapshot.dataset

        rows_per_loader = self.calculate_rows_per_loader(overall_rows_num=dataset["num_of_rows"])
        num_of_loaders = int(self.params.get("n_loaders"))

        cs_cmds = []
        cs_cmd_template = snapshot.cs_read_cmd_template

        for loader_index in range(num_of_loaders):
            sequence_start = rows_per_loader * loader_index + 1
            sequence_end = rows_per_loader * (loader_index + 1)

            cs_cmd = cs_cmd_template.format(
                cl=dataset["cl"],
                num_of_rows=rows_per_loader,
                keyspace_name=snapshot.keyspaces[0],
                replication=dataset["replication"],
                rf=dataset["rf"],
                compaction=dataset["compaction"],
                col_size=dataset["col_size"],
                col_n=dataset["col_n"],
                sequence_start=sequence_start,
                sequence_end=sequence_end,
            )
            cs_cmds.append(cs_cmd)

        return cs_cmds


@dataclass
class ManagerTestMetrics:
    backup_time = "N/A"
    restore_time = "N/A"


class DatabaseOperations(ClusterTester):

    def get_keyspace_name(self, ks_prefix: str = 'keyspace', ks_number: int = 1) -> list:
        """Get keyspace name based on the following logic:
            - if keyspaces number > 1, numeric indexes are used in the ks name;
            - if keyspaces number == 1, ks name depends on whether compression is applied to keyspace. If applied,
            the compression postfix will be used in the name, otherwise numeric index (keyspace1)
        """
        if ks_number > 1:
            return ['{}{}'.format(ks_prefix, i) for i in range(1, ks_number + 1)]
        else:
            stress_cmd = self.params.get('stress_read_cmd')
            if 'compression' in stress_cmd:
                compression_postfix = re.search('compression=(.*)Compressor', stress_cmd).group(1)
                keyspace_name = '{}_{}'.format(ks_prefix, compression_postfix.lower())
            else:
                keyspace_name = '{}{}'.format(ks_prefix, ks_number)
            return [keyspace_name]

    def get_table_id(self, node, table_name, keyspace_name=None, remove_hyphen=True):
        """

        :param keyspace_name: not mandatory. Should be used when there's more than one table with the same
        name in different keyspaces
        :param remove_hyphen: In the table's directory, scylla removes the hyphens from the id. Setting
        the attribute to True will remove the hyphens.
        """
        query = f"SELECT id FROM system_schema.tables WHERE table_name='{table_name}'"
        if keyspace_name:
            query += f"and keyspace_name='{keyspace_name}'"
        with self.db_cluster.cql_connection_patient(node) as session:
            results = session.execute(query)
        base_id = str(results[0].id)
        if remove_hyphen:
            return base_id.replace('-', '')
        return base_id

    def create_keyspace_and_basic_table(self, keyspace_name, table_name="example_table",
                                        replication_factor=1):
        self.log.info("creating keyspace {}".format(keyspace_name))
        keyspace_existence = self.create_keyspace(keyspace_name, replication_factor)
        assert keyspace_existence, "keyspace creation failed"
        # Keyspaces without tables won't appear in the repair, so the must have one
        self.log.info("creating the table {} in the keyspace {}".format(table_name, keyspace_name))
        self.create_table(table_name, keyspace_name=keyspace_name)

    def create_ks_and_tables(self, num_ks, num_table):
        # FIXME: beforehand we better change to have RF=1 to avoid restoring content while restoring replica of data
        table_name = []
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            for keyspace in range(num_ks):
                session.execute(f"CREATE KEYSPACE IF NOT EXISTS ks00{keyspace} "
                                "WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':1}")
                for table in range(num_table):
                    session.execute(f'CREATE COLUMNFAMILY IF NOT EXISTS ks00{keyspace}.table00{table} '
                                    '(key varchar, c varchar, v varchar, PRIMARY KEY(key, c))')
                    table_name.append(f'ks00{keyspace}.table00{table}')
                    # FIXME: improve the structure + data insertion
                    # can use this function to populate tables better?
                    # self.populate_data_parallel()
        return table_name

    def delete_keyspace_directory(self, db_node, keyspace_name):
        # Stop scylla service before deleting sstables to avoid partial deletion of files that are under compaction
        db_node.stop_scylla_server(verify_up=False, verify_down=True)

        try:
            directoy_path = f"/var/lib/scylla/data/{keyspace_name}"
            directory_size_result = db_node.remoter.sudo(f"du -h --max-depth=0 {directoy_path}")
            result = db_node.remoter.sudo(f'rm -rf {directoy_path}')
            if result.stderr:
                raise FilesNotCorrupted('Files were not corrupted. CorruptThenRepair nemesis can\'t be run. '
                                        'Error: {}'.format(result))
            if directory_size_result.stdout:
                directory_size = directory_size_result.stdout[:directory_size_result.stdout.find("\t")]
                self.log.debug("Removed the directory of keyspace {} from node {}\nThe size of the directory is {}".format(
                    keyspace_name, db_node, directory_size))

        finally:
            db_node.start_scylla_server(verify_up=True, verify_down=False)

    def insert_data_while_excluding_each_node(self, total_num_of_rows, keyspace_name="keyspace2"):
        """
        The function split the number of rows to the number of nodes (minus 1) and in loop does the following:
        shuts down one node, insert one part of the rows and starts the node again.
        As a result, each node that was shut down will have missing rows and will require repair.
        """
        num_of_nodes = self.params.get("n_db_nodes")
        num_of_rows_per_insertion = int(total_num_of_rows / (num_of_nodes - 1))
        stress_command_template = "cassandra-stress write cl=QUORUM n={} -schema 'keyspace={}" \
                                  " replication(strategy=NetworkTopologyStrategy,replication_factor=3)'" \
                                  " -col 'size=FIXED(1024) n=FIXED(1)' -pop seq={}..{} -mode cql3" \
                                  " native -rate threads=200 -log interval=5"
        start_of_range = 1
        # We can't shut down node 1 since it's the default contact point of the stress command, and we have no way
        # of changing that. As such, we skip it.
        for node in self.db_cluster.nodes[1:]:
            self.log.info("inserting {} rows to every node except {}".format(num_of_rows_per_insertion, node.name))
            end_of_range = start_of_range + num_of_rows_per_insertion - 1
            node.stop_scylla_server(verify_up=False, verify_down=True)
            stress_thread = self.run_stress_thread(stress_cmd=stress_command_template.format(num_of_rows_per_insertion,
                                                                                             keyspace_name,
                                                                                             start_of_range,
                                                                                             end_of_range))
            time.sleep(15)
            self.log.info('load={}'.format(stress_thread.get_results()))
            node.start_scylla_server(verify_up=True, verify_down=False)
            start_of_range = end_of_range + 1
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            session.execute(f"ALTER TABLE {keyspace_name}.standard1 WITH read_repair_chance = 0.0")

        for node in self.db_cluster.nodes:
            node.run_nodetool("flush")

    def create_missing_rows_in_cluster(self, create_missing_rows_in_multiple_nodes, total_num_of_rows,
                                       keyspace_to_be_repaired=None):
        if create_missing_rows_in_multiple_nodes:
            self.insert_data_while_excluding_each_node(total_num_of_rows=total_num_of_rows,
                                                       keyspace_name=keyspace_to_be_repaired)
            self.wait_no_compactions_running(n=40, sleep_time=10)
        else:
            target_node = self.db_cluster.nodes[2]
            self.delete_keyspace_directory(db_node=target_node, keyspace_name="keyspace1")


class StressLoadOperations(ClusterTester, LoaderUtilsMixin):

    def _generate_load(self, keyspace_name: str = None):
        self.log.info('Starting c-s write workload')
        stress_cmd = self.params.get('stress_cmd')

        # Handle list of stress commands using the pattern from LoaderUtilsMixin._run_all_stress_cmds
        stress_queue = []
        params = {'stress_cmd': stress_cmd, 'keyspace_name': keyspace_name, 'round_robin': False}
        self._run_all_stress_cmds(stress_queue, params)
        stress_thread = stress_queue[0] if stress_queue else None

        self.log.info('Sleeping for 15s to let cassandra-stress run...')
        time.sleep(15)
        return stress_thread

    def generate_load_and_wait_for_results(self, keyspace_name: str = None):
        load_thread = self._generate_load(keyspace_name=keyspace_name)
        load_results = load_thread.get_results()
        self.log.info(f'load={load_results}')

    def generate_background_read_load(self):
        self.log.info('Starting c-s read')
        stress_cmd = self.params.get('stress_read_cmd')
        number_of_nodes = self.params.get("n_db_nodes")
        number_of_loaders = self.params.get("n_loaders")

        throttle_per_node = 14666
        throttle_per_loader = int(throttle_per_node * number_of_nodes / number_of_loaders)

        # Handle list of stress commands - replace placeholder in each command
        if isinstance(stress_cmd, list):
            stress_cmd = [cmd.replace("<THROTTLE_PLACE_HOLDER>", str(throttle_per_loader)) for cmd in stress_cmd]
        else:
            stress_cmd = stress_cmd.replace("<THROTTLE_PLACE_HOLDER>", str(throttle_per_loader))

        # Handle list of stress commands using the pattern from LoaderUtilsMixin._run_all_stress_cmds
        stress_queue = []
        params = {'stress_cmd': stress_cmd, 'round_robin': False}
        self._run_all_stress_cmds(stress_queue, params)
        stress_thread = stress_queue[0] if stress_queue else None

        self.log.info('Sleeping for 15s to let cassandra-stress run...')
        time.sleep(15)
        return stress_thread

    def run_verification_read_stress(self, ks_names=None):
        stress_queue = []
        stress_cmd = self.params.get('stress_read_cmd')
        keyspace_num = self.params.get('keyspace_num')
        InfoEvent(message='Starting read stress for data verification').publish()
        stress_start_time = datetime.now()
        if ks_names:
            self.assemble_and_run_all_stress_cmd_by_ks_names(stress_queue, stress_cmd, ks_names)
        else:
            self.assemble_and_run_all_stress_cmd(stress_queue, stress_cmd, keyspace_num)
        for stress in stress_queue:
            self.verify_stress_thread(stress)
        stress_run_time = datetime.now() - stress_start_time
        InfoEvent(message=f'The read stress run was completed. Total run time: {stress_run_time}').publish()

    def run_and_verify_stress_in_threads(self, cs_cmds: list[str], stop_on_failure: bool = False) -> None:
        """Runs C-S commands in threads and verifies their execution results.
        Stress operation can be either read or write, depending on the cmd_template.
        """
        stress_queue = []
        for stress_cmd in cs_cmds:
            _thread = self.run_stress_thread(stress_cmd=stress_cmd, round_robin=True,
                                             stop_test_on_failure=stop_on_failure)
            stress_queue.append(_thread)

        for _thread in stress_queue:
            assert self.verify_stress_thread(_thread), "Stress thread verification failed"


class ManagerTestFunctionsMixIn(
    DatabaseOperations,
    StressLoadOperations,
    ClusterOperations,
    BucketOperations,
    SnapshotOperations,
    SnapshotPreparerOperations,
):
    test_config = TestConfig()
    manager_test_metrics = ManagerTestMetrics()

    def get_email_data(self):
        self.log.info("Prepare data for email")

        email_data = self._get_common_email_data()

        restore_parameters = self.params.get("mgmt_restore_extra_params")

        agent_backup_config = self.params.get("mgmt_agent_backup_config")
        if agent_backup_config:
            agent_backup_config = agent_backup_config.model_dump()

        email_data.update(
            {
                "manager_server_repo": self.params.get("scylla_mgmt_address"),
                "manager_agent_repo": (self.params.get("scylla_mgmt_agent_address") or
                                       self.params.get("scylla_mgmt_address")),
                "backup_time": str(self.manager_test_metrics.backup_time),
                "restore_time": str(self.manager_test_metrics.restore_time),
                "restore_parameters": restore_parameters,
                "agent_backup_config": agent_backup_config,
            }
        )
        return email_data

    @cached_property
    def locations(self) -> list[str]:
        backend = self.params.get("backup_bucket_backend")
        region = next(iter(self.params.region_names), '')
        bucket_locations = self.params.get("backup_bucket_location")

        buckets = (
            [bucket.format(region=region) for bucket in bucket_locations]
            if isinstance(bucket_locations, list)
            else bucket_locations.format(region=region).split()
        )

        # FIXME: Make it works with multiple locations or file a bug for scylla-manager.
        return [f"{backend}:{location}" for location in buckets[:1]]

    def get_dc_mapping(self) -> str | None:
        """Get the datacenter mapping string for the restore task if there are > 1 DCs (multiDC) in the cluster.
        In case of singleDC, return None.

        Example of return string:
            eu-west-2scylla_node_west=eu-west-2scylla_node_west,us-eastscylla_node_east=us-eastscylla_node_east
        """
        if len(dcs := self.get_all_dcs_names()) > 1:
            return ",".join([f"{dc}={dc}" for dc in dcs])
        else:
            return None

    def verify_backup_success(self, mgr_cluster, backup_task, ks_names: list = None, tables_names: list = None,
                              truncate=True, restore_data_with_task=False, timeout=None):
        if ks_names is None:
            ks_names = ['keyspace1']
        if tables_names is None:
            tables_names = ['standard1']
        ks_tables_map = {keyspace: tables_names for keyspace in ks_names}
        if truncate:
            for ks, tables in ks_tables_map.items():
                for table_name in tables:
                    self.log.info(f'running truncate on {ks}.{table_name}')
                    self.db_cluster.nodes[0].run_cqlsh(f'TRUNCATE {ks}.{table_name}')
        if restore_data_with_task:
            self.restore_backup_with_task(mgr_cluster=mgr_cluster, snapshot_tag=backup_task.get_snapshot_tag(),
                                          timeout=timeout, restore_data=True)
        else:
            snapshot_tag = backup_task.get_snapshot_tag()
            self.restore_backup_without_manager(mgr_cluster=mgr_cluster, snapshot_tag=snapshot_tag,
                                                ks_tables_list=ks_tables_map)

    def verify_alternator_backup_success(self, mgr_cluster, backup_task, delete_tables: list = None, timeout=None):
        for table_name in delete_tables:
            self.log.info(f'running delete on {table_name}')
            self.alternator.delete_table(self.db_cluster.nodes[0], table_name=table_name, wait_until_table_removed=True)
        self.restore_backup_with_task(mgr_cluster=mgr_cluster, snapshot_tag=backup_task.get_snapshot_tag(),
                                      timeout=timeout, restore_schema=True)
        self.restore_backup_with_task(mgr_cluster=mgr_cluster, snapshot_tag=backup_task.get_snapshot_tag(),
                                      timeout=timeout, restore_data=True)

    def restore_backup_without_manager(self, mgr_cluster, snapshot_tag, ks_tables_list, location=None,
                                       precreated_backup=False):
        """Restore backup without Scylla Manager but using the `nodetool refresh` operation
        (https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/refresh.html).

        The method downloads backup files from the backup bucket (aws s3 cp ...) and then loads them (nodetool refresh)
        into the cluster node by node and table by table.

        Two flows are supported:
          - restore to a new cluster from pre-created backup.
          - restore from backup created at the same cluster.
        The difference between these two flows is in the way the node_id is retrieved - if the backup was created with
        the cluster under test use node_ids of cluster under test, otherwise, get node_id from `sctool backup files...`
        """
        backup_bucket_backend = self.params.get("backup_bucket_backend")
        if backup_bucket_backend == "s3":
            install_dependencies = self.install_awscli_dependencies
            download = self.download_from_s3
        elif backup_bucket_backend == "gcs":
            install_dependencies = self.install_gsutil_dependencies
            download = self.download_from_gs
        elif backup_bucket_backend == "azure":
            install_dependencies = self.install_azcopy_dependencies
            download = self.download_from_azure
        else:
            raise ValueError(f'{backup_bucket_backend=} is not supported')

        browse_all_clusters = True if precreated_backup else False
        per_node_backup_file_paths = mgr_cluster.get_backup_files_dict(snapshot_tag, location, browse_all_clusters)
        # One path is for schema, it should be filtrated as this method is supposed to be run on restored schema cluster
        backed_up_node_ids = [i for i in per_node_backup_file_paths if 'schema' not in i]

        nodetool_refresh_extra_flags = self.params.get('mgmt_nodetool_refresh_flags') or ""
        for index, node in enumerate(self.db_cluster.nodes):
            install_dependencies(node=node)
            node_data_path = Path("/var/lib/scylla/data")
            # If the backup was not created with the cluster under test (precreated backup), get node_id from
            # sctool backup files output, otherwise, use node_ids of cluster under test
            backed_up_node_id = backed_up_node_ids[index] if precreated_backup else node.host_id
            for keyspace, tables in ks_tables_list.items():
                keyspace_path = node_data_path / keyspace
                for table in tables:
                    table_id = self.get_table_id(node=node, table_name=table, keyspace_name=keyspace)
                    table_upload_path = keyspace_path / f"{table}-{table_id}" / "upload"

                    with ExecutionTimer() as timer:
                        for file_path in per_node_backup_file_paths[backed_up_node_id][keyspace][table]:
                            download(node=node, source=file_path, destination=table_upload_path)
                    self.log.info(f"[Node {index}][{keyspace}.{table}] Download took {timer.duration}")

                    node.remoter.sudo(f"chown scylla:scylla -Rf {table_upload_path}")

                    with ExecutionTimer() as timer:
                        node.run_nodetool(f"refresh {keyspace} {table} {nodetool_refresh_extra_flags}")
                    self.log.info(f"[Node {index}][{keyspace}.{table}] Nodetool refresh took {timer.duration}")

    def restore_backup_with_task(self, mgr_cluster, snapshot_tag, timeout, restore_schema=False, restore_data=False,
                                 location_list=None, extra_params=None, object_storage_method=None):
        location_list = location_list if location_list else self.locations
        dc_mapping = self.get_dc_mapping() if restore_data else None
        restore_task = mgr_cluster.create_restore_task(restore_schema=restore_schema, restore_data=restore_data,
                                                       location_list=location_list, snapshot_tag=snapshot_tag,
                                                       dc_mapping=dc_mapping, extra_params=extra_params, object_storage_method=object_storage_method)
        restore_task.wait_and_get_final_status(step=30, timeout=timeout)
        assert restore_task.status == TaskStatus.DONE, f"Restoration of {snapshot_tag} has failed!"
        InfoEvent(message=f'The restore task has ended successfully. '
                  f'Restore run time: {restore_task.duration}.').publish()

        should_restart = restore_schema and ComparableScyllaVersion(self.db_cluster.nodes[0].scylla_version) <= "2024.1"
        if should_restart:
            self.db_cluster.restart_scylla()

        return restore_task

    def create_repair_and_alter_it_with_repair_control(self):
        keyspace_to_be_repaired = "keyspace2"
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = manager_tool.add_cluster(name=self.CLUSTER_NAME + '_repair_control',
                                               db_cluster=self.db_cluster,
                                               auth_token=self.monitors.mgmt_auth_token)
        # writing 292968720 rows, equal to the amount of data written in the prepare (around 100gb per node),
        # to create a large data fault and therefore a longer running repair
        self.create_missing_rows_in_cluster(create_missing_rows_in_multiple_nodes=True,
                                            keyspace_to_be_repaired=keyspace_to_be_repaired,
                                            total_num_of_rows=292968720)
        arg_list = [{"intensity": .0001},
                    {"intensity": 0},
                    {"parallel": 1},
                    {"intensity": 2, "parallel": 1}]

        InfoEvent(message="Repair started").publish()
        repair_task = mgr_cluster.create_repair_task(keyspace="keyspace2")
        next_percentage_block = 20
        repair_task.wait_for_percentage(next_percentage_block)
        for args in arg_list:
            next_percentage_block += 20
            InfoEvent(message=f"Changing repair args to: {args}").publish()
            mgr_cluster.control_repair(**args)
            repair_task.wait_for_percentage(next_percentage_block)
        repair_task.wait_and_get_final_status(step=30)
        InfoEvent(message="Repair ended").publish()


def run_manager_backup(mgr_cluster, locations, object_storage_upload_mode=None, timeout=7200):
    """
    Perform a Manager backup task and wait for its completion.

    Args:
        mgr_cluster: The ManagerCluster object.
        locations: List of backup locations.
        object_storage_upload_mode: The upload mode (e.g., RCLONE or NATIVE).
        timeout: Timeout for the backup task.

    Returns:
        The completed backup task.
    """
    InfoEvent(
        message=f'Starting a Manager backup (Object Storage Upload Mode: {object_storage_upload_mode})').publish()
    task = mgr_cluster.create_backup_task(location_list=locations, rate_limit_list=["0"],
                                          object_storage_upload_mode=object_storage_upload_mode)
    backup_status = task.wait_and_get_final_status(timeout=timeout)
    assert backup_status == TaskStatus.DONE, "Backup upload has failed!"
    return task
