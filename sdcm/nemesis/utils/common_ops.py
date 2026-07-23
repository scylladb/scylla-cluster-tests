"""
Cross-cutting utility functions shared by multiple nemesis groups.

Functions here are stateless helpers that accept a ``NemesisRunner`` (or its
constituent parts) and perform an operation.  They are imported by individual
monkey modules so that the logic lives in one place.
"""

import random

from sdcm.exceptions import FilesNotCorrupted, UnsupportedNemesis
from sdcm.sct_events.group_common_events import (
    decorate_with_context,
    suppress_expected_unavailability_errors,
)
from sdcm.utils.context_managers import DbNodeLogger


@decorate_with_context(suppress_expected_unavailability_errors)
def destroy_data_and_restart_scylla(runner, keyspaces_for_destroy: list = None, sstables_to_destroy_perc: int = 50):
    """Stop Scylla, randomly destroy a percentage of SStables, and restart.

    This is a cross-group utility used by manager, data-destroy, and streaming
    nemesis.  It was originally ``NemesisRunner._destroy_data_and_restart_scylla``.

    Args:
        runner: A ``NemesisRunner`` instance (provides ``cluster``,
            ``target_node``, ``log``, ``actions_log``, ``action_log_scope``,
            ``get_all_sstables``, and ``replace_full_file_name_to_prefix``).
        keyspaces_for_destroy: Optional list of keyspaces to limit destruction.
        sstables_to_destroy_perc: Percentage of SStables to remove (default 50).
    """
    tables = runner.cluster.get_non_system_ks_cf_list(
        db_node=runner.target_node, filter_empty_tables=False, filter_by_keyspace=keyspaces_for_destroy
    )
    if not tables:
        raise UnsupportedNemesis("Non-system keyspace and table are not found. The nemesis can't be run")

    runner.log.debug("Chosen tables: %s", tables)

    # Stop scylla service before deleting sstables to avoid partial deletion of files that are under compaction
    with runner.action_log_scope(f"Stop Scylla on {runner.target_node.name}"):
        runner.target_node.stop_scylla_server(verify_up=False, verify_down=True)

    try:
        # Remove data files
        if not (all_files_to_destroy := runner.get_all_sstables(tables=tables, node=runner.target_node)):
            raise UnsupportedNemesis("SStables for destroy are not found. The nemesis can't be run")

        # How many SStables are going to be deleted
        sstables_amount_to_destroy = int(len(all_files_to_destroy) * sstables_to_destroy_perc / 100)
        runner.log.debug(
            "SStables amount to destroy (%s percent of all SStables): %s",
            sstables_to_destroy_perc,
            sstables_amount_to_destroy,
        )

        destroyed_files = 0
        while sstables_amount_to_destroy > 0:
            file_for_destroy = random.choice(all_files_to_destroy)
            if not (
                file_group_for_destroy := runner.replace_full_file_name_to_prefix(
                    one_file=file_for_destroy, ks_cf_for_destroy=tables
                )
            ):
                continue

            with DbNodeLogger(
                runner.cluster.nodes,
                "remove data",
                target_node=runner.target_node,
                additional_info=file_group_for_destroy,
            ):
                result = runner.target_node.remoter.sudo("rm -f %s" % file_group_for_destroy)
            if result.stderr:
                raise FilesNotCorrupted(f"Files were not removed. The nemesis can't be run. Error: {result}")
            all_files_to_destroy.remove(file_for_destroy)
            sstables_amount_to_destroy -= 1
            destroyed_files += 1
            runner.log.debug(f"Files {file_for_destroy} were destroyed")
        runner.actions_log.info(f"removed {destroyed_files} files in tables: {tables} on {runner.target_node.name}")

    finally:
        with runner.action_log_scope(f"Start Scylla on {runner.target_node.name} node"):
            runner.target_node.start_scylla_server(verify_up=True, verify_down=False)
