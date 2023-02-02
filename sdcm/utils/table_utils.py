import logging

from sdcm import wait
from sdcm.utils.decorators import retrying


class NoSstableFound(Exception):
    pass


LOGGER = logging.getLogger(__name__)


def wait_until_user_table_exists(db_node, table_name: str = 'random', timeout_min: int = 20):
    text = f'Waiting until {table_name} user table exists'
    db_cluster = db_node.parent_cluster
    if table_name.lower() == 'random':
        wait.wait_for(func=lambda: len(db_cluster.get_non_system_ks_cf_list(db_node)) > 0, step=60,
                      text=text, timeout=60 * timeout_min, throw_exc=True)
    else:
        wait.wait_for(func=lambda: table_name in (db_cluster.get_non_system_ks_cf_list(db_node)), step=60,
                      text=text, timeout=60 * timeout_min, throw_exc=True)


@retrying(n=10, allowed_exceptions=NoSstableFound)
def get_sstables(db_node, ks_cf: str, from_minutes_ago: int = 0):
    ks_cf_path = ks_cf.replace('.', '/')
    find_cmd = f"find /var/lib/scylla/data/{ks_cf_path}-*/*-big-Data.db -maxdepth 1 -type f"
    if from_minutes_ago:
        find_cmd += f" -cmin -{from_minutes_ago}"
    sstables_res = db_node.remoter.sudo(find_cmd, verbose=True)  # TODO: change to verbose=False?
    if sstables_res.stderr:
        raise NoSstableFound(
            'Failed to get sstables for {}. Error: {}'.format(ks_cf_path, sstables_res.stderr))

    selected_sstables = sstables_res.stdout.split()
    LOGGER.debug('Got %s sstables filtered by last %s minutes', len(selected_sstables),
                 from_minutes_ago)
    return selected_sstables


def count_sstable_tombstones(db_node, sstable: str):
    db_node.remoter.run(f'sudo sstabledump  {sstable} 1>/tmp/sstabledump.json', verbose=True)
    tombstones_deletion_info = db_node.remoter.run(
        'sudo grep marked_deleted /tmp/sstabledump.json').stdout.splitlines()
    return len(tombstones_deletion_info)
