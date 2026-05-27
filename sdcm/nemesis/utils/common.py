"""
Common cross-cutting helpers shared across multiple nemesis groups.

These are stateless functions that accept explicit parameters instead of relying on `self`.
"""

import logging

from sdcm.exceptions import NemesisStressFailure

LOGGER = logging.getLogger(__name__)


def prepare_test_table(tester, ks="keyspace1", stress_failure_handler=None):
    """
    Populate ``<ks>.standard1`` with 400 K rows via cassandra-stress.

    Args:
        tester: ClusterTester instance (used to run stress and get replication factor)
        ks: keyspace name for the cassandra-stress default table
        stress_failure_handler: optional error handler callback for stress thread verification
    """
    stress_cmd = (
        "cassandra-stress write n=400000 cl=QUORUM -mode native cql3 "
        f"-schema 'keyspace={ks} replication(strategy=NetworkTopologyStrategy,"
        f"replication_factor={tester.reliable_replication_factor})' -log interval=5"
    )
    cs_thread = tester.run_stress_thread(stress_cmd=stress_cmd, stop_test_on_failure=False, round_robin=True)
    tester.verify_stress_thread(cs_thread, error_handler=stress_failure_handler)


def nemesis_stress_failure_handler(nemesis_name, stress_pool, errors):
    """
    Error handler for nemesis stress — aborts if stress failed on all loaders.

    Args:
        nemesis_name: Name of the nemesis (for error message)
        stress_pool: pool of stress threads
        errors: dict of errors per loader node
    """
    if len(errors) == len(stress_pool.get_results()):
        errors_str = "".join(f" on node '{node_name}': {errs}\n" for node_name, errs in errors.items())
        raise NemesisStressFailure(
            f"Aborting '{nemesis_name}' nemesis as stress command failed with the following errors:\n{errors_str}"
        )
