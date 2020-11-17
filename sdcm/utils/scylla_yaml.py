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


import yaml

from sdcm.utils import alternator


def get_scylla_yaml_from_config(
        seed_address=None,
        cluster_name=None,
        enable_exp=True,
        endpoint_snitch=None,
        broadcast=None,
        authenticator=None,
        server_encrypt=None,
        client_encrypt=None,
        append_scylla_yaml=None,
        hinted_handoff="enabled",
        murmur3_partitioner_ignore_msb_bits=None,
        authorizer=None,
        alternator_port=None,
        listen_on_all_interfaces=False,
        ip_ssh_connections=None,
        alternator_enforce_authorization=False,
        internode_compression=None,
        internode_encryption=None,
        private_ip_address=None,
        ip_address=None,
        enable_auto_bootstrap=None,
        replacement_node_ip=None,
        scylla_yaml=None,
        **unused_config_params) -> dict:

    if scylla_yaml is None:
        scylla_yaml = {}
    if seed_address and private_ip_address:
        scylla_yaml['seed_provider'] = [
            dict(class_name='org.apache.cassandra.locator.SimpleSeedProvider',
                 parameters=[dict(seeds=seed_address)])]

        # NOTICE: the following configuration always have to use private_ip_address for multi-region to work
        scylla_yaml['listen_address'] = private_ip_address
        scylla_yaml['rpc_address'] = private_ip_address

    if listen_on_all_interfaces:
        scylla_yaml['listen_address'] = "0.0.0.0"
        scylla_yaml['rpc_address'] = "0.0.0.0"

    if broadcast:
        scylla_yaml['broadcast_address'] = broadcast
        scylla_yaml['broadcast_rpc_address'] = broadcast

    if cluster_name:
        scylla_yaml['cluster_name'] = cluster_name

    # disable hinted handoff (it is enabled by default in Scylla). Expected values: "enabled"/"disabled"
    if hinted_handoff == 'disabled':
        scylla_yaml['hinted_handoff_enabled'] = False

    if ip_ssh_connections == 'ipv6' and ip_address:
        scylla_yaml['enable_ipv6_dns_lookup'] = True
        scylla_yaml['prometheus_address'] = ip_address
        scylla_yaml['broadcast_rpc_address'] = ip_address
        scylla_yaml['listen_address'] = ip_address
        scylla_yaml['rpc_address'] = ip_address

    if murmur3_partitioner_ignore_msb_bits:
        scylla_yaml['murmur3_partitioner_ignore_msb_bits'] = int(murmur3_partitioner_ignore_msb_bits)

    if enable_exp:
        scylla_yaml['experimental'] = True

    if endpoint_snitch:
        scylla_yaml['endpoint_snitch'] = endpoint_snitch

    if not client_encrypt:
        scylla_yaml['client_encryption_options'] = dict(enabled=False)

    if enable_auto_bootstrap:
        scylla_yaml['auto_bootstrap'] = True
    else:
        if 'auto_bootstrap' in scylla_yaml:
            scylla_yaml['auto_bootstrap'] = False

    if authenticator in ['AllowAllAuthenticator', 'PasswordAuthenticator']:
        scylla_yaml['authenticator'] = authenticator

    if authorizer in ['AllowAllAuthorizer', 'CassandraAuthorizer']:
        scylla_yaml['authorizer'] = authorizer

    if server_encrypt:
        scylla_yaml['server_encryption_options'] = dict(internode_encryption=internode_encryption,
                                                       certificate='/etc/scylla/ssl_conf/db.crt',
                                                       keyfile='/etc/scylla/ssl_conf/db.key',
                                                       truststore='/etc/scylla/ssl_conf/cadb.pem')

    if client_encrypt:
        scylla_yaml['client_encryption_options'] = dict(enabled=True,
                                                       certificate='/etc/scylla/ssl_conf/client/test.crt',
                                                       keyfile='/etc/scylla/ssl_conf/client/test.key',
                                                       truststore='/etc/scylla/ssl_conf/client/catest.pem')

    if replacement_node_ip:
        scylla_yaml['replace_address_first_boot'] = replacement_node_ip
    else:
        if 'replace_address_first_boot' in scylla_yaml:
            del scylla_yaml['replace_address_first_boot']

    if alternator_port:
        scylla_yaml['alternator_port'] = alternator_port
        scylla_yaml['alternator_write_isolation'] = alternator.enums.WriteIsolation.ALWAYS_USE_LWT.value

    if alternator_enforce_authorization:
        scylla_yaml['alternator_enforce_authorization'] = True
    else:
        scylla_yaml['alternator_enforce_authorization'] = False

    if internode_compression:
        scylla_yaml['internode_compression'] = internode_compression

    if append_scylla_yaml:
        scylla_yaml.update(yaml.safe_load(append_scylla_yaml))
    return scylla_yaml


def get_config_from_params(seed_address=None, endpoint_snitch=None, murmur3_partitioner_ignore_msb_bits=None,
                           client_encrypt=None, params=None, name=None, append_scylla_args=None) -> dict:
    return {
        'seed_address': seed_address,
        'cluster_name': name,  # pylint: disable=no-member
        'enable_exp': params.get('experimental'),
        'endpoint_snitch': endpoint_snitch,
        'authenticator': params.get('authenticator'),
        'server_encrypt': params.get('server_encrypt'),
        'client_encrypt': client_encrypt if client_encrypt is not None else params.get('client_encrypt'),
        'append_scylla_yaml': params.get('append_scylla_yaml'),
        'append_scylla_args': append_scylla_args,
        'hinted_handoff': params.get('hinted_handoff'),
        'authorizer': params.get('authorizer'),
        'alternator_port': params.get('alternator_port'),
        'murmur3_partitioner_ignore_msb_bits': murmur3_partitioner_ignore_msb_bits,
        'alternator_enforce_authorization': params.get('alternator_enforce_authorization'),
        'internode_compression': params.get('internode_compression'),
        'internode_encryption': params.get('internode_encryption')}

