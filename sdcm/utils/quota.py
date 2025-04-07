from __future__ import absolute_import, annotations

import logging
import time
from contextlib import contextmanager

from sdcm.exceptions import QuotaConfigurationFailure
from sdcm.wait import wait_for


LOGGER = logging.getLogger(__name__)


def enable_quota_on_node(node):
    if not is_quota_enabled_on_node(node):
        LOGGER.info('Enabling quota on node {}'.format(node.name))
        change_default_grub_cmd = 'sed -i s\'/GRUB_CMDLINE_LINUX_DEFAULT="quiet splash"/' \
                                  'GRUB_CMDLINE_LINUX_DEFAULT="quiet splash rootflags=uquota,pquota"/\' ' \
                                  '/etc/default/grub'
        mk_grub_cmd = 'grub-mkconfig -o /boot/grub/grub.cfg'
        enable_uquota_on_mount_cmd = 'sed -i s\'/Options=noatime,discard/Options=noatime,discard,uquota/\' ' \
                                     '/etc/systemd/system/var-lib-scylla.mount'

        node.remoter.sudo(cmd=change_default_grub_cmd)
        node.remoter.sudo(cmd=mk_grub_cmd)
        node.remoter.sudo(cmd=enable_uquota_on_mount_cmd)
        LOGGER.debug('Rebooting node: "{}"'.format(node.name))
        node.reboot(hard=False)
        node.wait_node_fully_start()


def is_quota_enabled_on_node(node):
    """ Verify usrquota is enabled on scylla user. """
    LOGGER.info("Verifying quota is configured on the node {}".format(node.name))
    verify_usrquot_in_mount_cmd = "cat /proc/mounts | grep /var/lib/scylla | awk {'print $4'}"
    result = node.remoter.run(cmd=verify_usrquot_in_mount_cmd).stdout.strip()
    if "usrquota" not in result:
        LOGGER.warning("User quota on user scylla at /var/lib/scylla is not enabled")
        return False
    return True


def get_currently_used_space_by_scylla_user(node) -> int:
    result = node.remoter.sudo("xfs_quota -x -c 'report' /var/lib/scylla | grep scylla | tail -1")
    used_space = int(result.stdout.split()[1])
    return used_space


def get_quota_size_to_configure(node):
    used_space = get_currently_used_space_by_scylla_user(node)
    LOGGER.debug('Currently used space by scylla user is: {}K'.format(used_space))
    return used_space + 3000000


@contextmanager
def configure_quota_on_node_for_scylla_user_context(node):
    """
    Configure quota with current disk usage size by scylla user + 3GB on /var/lib/scylla mount point
    Verify quota was configured
    yield the quota size
    Remove quota configuration
    """
    quota_size = get_quota_size_to_configure(node)
    LOGGER.info("Configuring quota for scylla user with quota size: {}K on node {}".format(quota_size, node))
    conf_quota_cmd = f"xfs_quota -x -c 'limit bsoft={quota_size}K bhard={quota_size}K scylla' /var/lib/scylla"
    node.remoter.sudo(conf_quota_cmd, verbose=True)
    verify_quota_is_configured_for_scylla_user(node)
    try:
        yield quota_size
    finally:
        remove_quota_configuration_from_scylla_user_on_node(node)


def verify_quota_is_configured_for_scylla_user(node):
    """Verifies quota ia actually configured for scylla user, but doesn't verify enforcement in ON"""
    get_scylla_user_quota_value = 'xfs_quota -x -c "report -h" /var/lib/scylla | grep scylla | tail -1 | awk {\'print $4\'}'
    LOGGER.info("Verifying quota is configured for scylla user")
    quota_value = node.remoter.sudo(get_scylla_user_quota_value).stdout.rstrip()
    if quota_value == "0":
        LOGGER.error("Failed to configure quota on user")
        raise QuotaConfigurationFailure(
            "Quota was not configured for scylla user. The value for hard quota is: {}".format(quota_value))
    LOGGER.info("Quota was configured for scylla user with value: {}".format(quota_value))


def remove_quota_configuration_from_scylla_user_on_node(node):
    LOGGER.info('Remove quota configuration from user \'scylla\' by setting it to 0')
    quota_remove_cmd = "xfs_quota -x -c 'limit bsoft=0 bhard=0 scylla' /var/lib/scylla"
    node.remoter.sudo(quota_remove_cmd, verbose=True)


def write_data_to_reach_end_of_quota(node, quota_size):
    LOGGER.debug('Watching the log for the relevant message for quota...')
    quota_exceeded_appearances = node.follow_system_log(
        patterns=[r'Shutting down communications due to I\/O errors until operator intervention: Disk error:.*Disk quota exceeded'])
    node.remoter.sudo('chsh -s /bin/bash scylla')  # Enable login for scylla user

    def approach_end_of_quota():
        if bool(list(quota_exceeded_appearances)):
            return True
        currently_used_space = get_currently_used_space_by_scylla_user(node)
        occupy_space_size = int((quota_size - currently_used_space) * 90 / 100)
        occupy_space_cmd = f"su - scylla -c 'fallocate -l {occupy_space_size}K /var/lib/scylla/occupy_90percent.{time.time()}'"
        try:
            LOGGER.debug('Cost 90% free space on /var/lib/scylla/ by {}'.format(occupy_space_cmd))
            node.remoter.sudo(occupy_space_cmd, Verbose=True)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("We should have reached the expected I/O error and quota has exceeded\n"
                           "Message: {}".format(str(exc)))
        return bool(list(quota_exceeded_appearances))

    try:
        wait_for(func=approach_end_of_quota,
                 timeout=300,
                 step=10,
                 text="Waiting for 'Disk error: std::system_error (error system:122, Disk quota exceeded)' "
                 "string to appear in the node log",
                 throw_exc=False
                 )
        LOGGER.debug('Sleep 5 minutes before releasing space to scylla')
        time.sleep(300)

    finally:
        LOGGER.debug('Delete occupy_90percent file to release space to scylla-server')
        node.remoter.sudo('rm -rf /var/lib/scylla/occupy_90percent.*')  # Remove the sparse files

        LOGGER.debug('Disable scylla user login')
        node.remoter.sudo('usermod -s /sbin/nologin scylla')  # Disable login for scylla user
