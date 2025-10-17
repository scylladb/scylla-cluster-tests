from sdcm.utils.alternator import enums, consts
from sdcm.utils.alternator.api import Alternator
from contextlib import contextmanager


def pre_create_alternator_backuped_tables(node, alternator: Alternator, params, **kwargs):
    if params.get('alternator_port'):
        alternator.set_credentials(node=node)

        schema = params.get("dynamodb_primarykey_type")
        table_name = params.get('alternator_test_table').get('name', consts.NO_LWT_TABLE_NAME)
        alternator.create_table(node=node, schema=schema, isolation=enums.WriteIsolation.FORBID_RMW,
                                table_name=table_name, **kwargs)

        stress_cmd = params.get('stress_cmd')
        stress_read_cmd = params.get('stress_read_cmd')
        is_ttl_in_workload = any('dynamodb.ttlKey' in str(cmd) for cmd in [stress_cmd, stress_read_cmd])
        if is_ttl_in_workload:
            alternator.update_table_ttl(node=node, table_name=table_name)

        return {table_name: schema}
    return None


@contextmanager
def alternator_backuped_tables(node, alternator: Alternator, params, **kwargs):
    tables = pre_create_alternator_backuped_tables(node, alternator, params, **kwargs)
    yield tables
    if tables is not None:
        for table in tables.keys():
            alternator.delete_table(node=node, table_name=table)
