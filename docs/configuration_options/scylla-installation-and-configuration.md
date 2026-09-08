# Scylla installation and configuration

[← All configuration options](configuration_options.md)

Which Scylla to install and how it is configured: repos, versions, distro,
`scylla.yaml`/command-line options, experimental features, authentication and encryption.

**44 options.** Jump to: [append_scylla_args](#append_scylla_args) · [append_scylla_node_exporter_args](#append_scylla_node_exporter_args) · [append_scylla_setup_args](#append_scylla_setup_args) · [append_scylla_yaml](#append_scylla_yaml) · [assert_linux_distro_features](#assert_linux_distro_features) · [authenticator](#authenticator) · [authenticator_password](#authenticator_password) · [authenticator_user](#authenticator_user) · [authorizer](#authorizer) · [client_encrypt](#client_encrypt) · [client_encrypt_mtls](#client_encrypt_mtls) · [db_type](#db_type) · [enable_kms_key_rotation](#enable_kms_key_rotation) · [endpoint_snitch](#endpoint_snitch) · [enterprise_disable_kms](#enterprise_disable_kms) · [experimental_features](#experimental_features) · [hinted_handoff](#hinted_handoff) · [install_mode](#install_mode) · [internode_compression](#internode_compression) · [internode_encryption](#internode_encryption) · [jmx_heap_memory](#jmx_heap_memory) · [kms_key_rotation_interval](#kms_key_rotation_interval) · [ldap_server_type](#ldap_server_type) · [nonroot_offline_install](#nonroot_offline_install) · [peer_verification](#peer_verification) · [prepare_saslauthd](#prepare_saslauthd) · [scylla_apt_keys](#scylla_apt_keys) · [scylla_d_overrides_files](#scylla_d_overrides_files) · [scylla_encryption_options](#scylla_encryption_options) · [scylla_linux_distro](#scylla_linux_distro) · [scylla_linux_distro_loader](#scylla_linux_distro_loader) · [scylla_network_config](#scylla_network_config) · [scylla_repo](#scylla_repo) · [scylla_version](#scylla_version) · [server_encrypt](#server_encrypt) · [server_encrypt_mtls](#server_encrypt_mtls) · [service_level_shares](#service_level_shares) · [unified_package](#unified_package) · [update_db_packages](#update_db_packages) · [use_ldap](#use_ldap) · [use_ldap_authentication](#use_ldap_authentication) · [use_ldap_authorization](#use_ldap_authorization) · [use_preinstalled_scylla](#use_preinstalled_scylla) · [user_data_format_version](#user_data_format_version)


## **append_scylla_args** / SCT_APPEND_SCYLLA_ARGS

More arguments to append to scylla command line

**default:** --blocked-reactor-notify-ms 25 --abort-on-lsa-bad-alloc 1 --abort-on-seastar-bad-alloc --abort-on-internal-error 1 --abort-on-ebadf 1 --enable-sstable-key-validation 1

**type:** str (appendable)

**backend overrides:**
- `--abort-on-lsa-bad-alloc 1 --abort-on-internal-error 1 --abort-on-ebadf 1 --enable-sstable-key-validation 1`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **append_scylla_node_exporter_args** / SCT_APPEND_SCYLLA_NODE_EXPORTER_ARGS

More arguments to append to scylla-node-exporter command line

**default:** N/A

**type:** str (appendable)


## **append_scylla_setup_args** / SCT_APPEND_SCYLLA_SETUP_ARGS

More arguments to append to scylla_setup command line

**default:** N/A

**type:** str (appendable)


## **append_scylla_yaml** / SCT_APPEND_SCYLLA_YAML

More configuration to append to /etc/scylla/scylla.yaml

**default:** {'rf_rack_valid_keyspaces': True}

**type:** dict | str | pydantic.main.BaseModel


## **assert_linux_distro_features** / SCT_ASSERT_LINUX_DISTRO_FEATURES

List of distro features relevant to SCT test. Example: 'fips'.<br>This is used to assert that the distro features are supported by the scylla version being tested.<br>If the feature is not supported, the test will fail.

**default:** []

**type:** str | list[str] → list[str] (appendable)


## **authenticator** / SCT_AUTHENTICATOR

which authenticator scylla will use AllowAllAuthenticator/PasswordAuthenticator

**default:** N/A

**type:** Literal['PasswordAuthenticator', 'AllowAllAuthenticator', 'com.scylladb.auth.SaslauthdAuthenticator']


## **authenticator_password** / SCT_AUTHENTICATOR_PASSWORD

the password if PasswordAuthenticator is used

**default:** N/A

**type:** str (appendable)


## **authenticator_user** / SCT_AUTHENTICATOR_USER

the username if PasswordAuthenticator is used

**default:** N/A

**type:** str (appendable)


## **authorizer** / SCT_AUTHORIZER

which authorizer scylla will use AllowAllAuthorizer/CassandraAuthorizer

**default:** N/A

**type:** Literal['AllowAllAuthorizer', 'CassandraAuthorizer']


## **client_encrypt** / SCT_CLIENT_ENCRYPT

when enable scylla will use encryption on the client side

**default:** False

**type:** bool


## **client_encrypt_mtls** / SCT_CLIENT_ENCRYPT_MTLS

when enabled scylla will enforce mutual authentication when client-to-node encryption is enabled

**default:** False

**type:** bool


## **db_type** / SCT_DB_TYPE

Db type to install into db nodes, scylla/cassandra

**default:** scylla

**type:** str (appendable)


## **enable_kms_key_rotation** / SCT_ENABLE_KMS_KEY_ROTATION

Allows to disable KMS keys rotation. Applicable to AWS, GCP, and Azure backends.

**default:** True

**type:** bool


## **endpoint_snitch** / SCT_ENDPOINT_SNITCH

The snitch class scylla would use<br><br>'GossipingPropertyFileSnitch' - default<br>'Ec2MultiRegionSnitch' - default on aws backend<br>'GoogleCloudSnitch'

**default:** N/A

**type:** str (appendable)


## **enterprise_disable_kms** / SCT_ENTERPRISE_DISABLE_KMS

An escape hatch to disable KMS for enterprise run, when needed. We enable KMS by default since if we use Scylla 2023.1.3 and up

**default:** False

**type:** bool


## **experimental_features** / SCT_EXPERIMENTAL_FEATURES

Scylla experimental features to enable in scylla.yaml, as a list of feature names (e.g. 'udf', 'alternator-streams').

**default:** []

**type:** str | list[str] → list[str] (appendable)


## **hinted_handoff** / SCT_HINTED_HANDOFF

when enable or disable scylla hinted handoff (enabled/disabled)

**default:** disabled

**type:** str (appendable)


## **install_mode** / SCT_INSTALL_MODE

Scylla install mode, repo/offline/web

**default:** repo

**type:** str


## **internode_compression** / SCT_INTERNODE_COMPRESSION

Scylla [`internode_compression`](#internode_compression) in scylla.yaml: which inter-node traffic to compress -- 'all', 'dc' (between datacenters only) or 'none'.

**default:** N/A

**type:** str (appendable)


## **internode_encryption** / SCT_INTERNODE_ENCRYPTION

Scylla sub option of server_encryption_options: internode_encryption.

**default:** all

**type:** str (appendable)


## **jmx_heap_memory** / SCT_JMX_HEAP_MEMORY

The total size of the memory allocated to JMX. Values in MB, so for 1GB enter 1024(MB).

**default:** N/A

**type:** int


## **kms_key_rotation_interval** / SCT_KMS_KEY_ROTATION_INTERVAL

The time interval in minutes which gets waited before the KMS key rotation happens. Applied when the AWS KMS service is configured to be used.

**default:** N/A

**type:** int

**backend overrides:**
- `60`: aws, gce, azure, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **ldap_server_type** / SCT_LDAP_SERVER_TYPE

This option indicates which server is going to be used for LDAP operations. [openldap, ms_ad]

**default:** N/A

**type:** str (appendable)


## **nonroot_offline_install** / SCT_NONROOT_OFFLINE_INSTALL

Install Scylla without required root privilege

**default:** N/A

**type:** bool


## **peer_verification** / SCT_PEER_VERIFICATION

enable peer verification for encrypted communication

**default:** True

**type:** bool


## **prepare_saslauthd** / SCT_PREPARE_SASLAUTHD

When defined true, will install and start saslauthd service

**default:** N/A

**type:** bool


## **scylla_apt_keys** / SCT_SCYLLA_APT_KEYS

APT keys for ScyllaDB repos

**default:** ['17723034C56D4B19', '5E08FBD8B5D6EC9C', 'D0A112E067426AB2', '491C93B9DE7496A7', 'A43E06657BAC99E3', 'C503C686B007F39E']

**type:** str | list[str] → list[str] (appendable)


## **scylla_d_overrides_files** / SCT_SCYLLA_D_OVERRIDES_FILES

list of files that should upload to /etc/scylla.d/ directory to override scylla config files

**default:** []

**type:** str | list[str] → list[str] (appendable)


## **scylla_encryption_options** / SCT_SCYLLA_ENCRYPTION_OPTIONS

options will be used for enable encryption at-rest for tables

**default:** N/A

**type:** str (appendable)


## **scylla_linux_distro** / SCT_SCYLLA_LINUX_DISTRO

Distro and family for the DB node image, e.g. 'ubuntu-jammy' or 'debian-bookworm'.

**default:** ubuntu-focal

**type:** str

**backend overrides:**
- `centos`: docker


## **scylla_linux_distro_loader** / SCT_SCYLLA_LINUX_DISTRO_LOADER

Distro and family for the loader node image. Independent of the DB nodes, so loaders can run a different distro.

**default:** ubuntu-jammy

**type:** str


## **scylla_network_config** / SCT_SCYLLA_NETWORK_CONFIG

Configure Scylla networking with single or multiple NIC/IP combinations.<br>It must be defined for listen_address and rpc_address. For each address mandatory parameters are:<br>- address: listen_address/rpc_address/broadcast_rpc_address/broadcast_address/test_communication<br>- ip_type: ipv4 or ipv6<br>- public: false or true<br>- nic: number of NIC. 0, 1<br>Supported for AWS and GCE meanwhile

**default:** N/A

**type:** list

**backend overrides:**
- `[{'address': 'listen_address', 'listen_all': False, 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}, {'address': 'rpc_address', 'listen_all': False, 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}, {'address': 'broadcast_rpc_address', 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}, {'address': 'broadcast_address', 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}, {'address': 'test_communication', 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}]`: aws, gce, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **scylla_repo** / SCT_SCYLLA_REPO

Url to the repo of scylla version to install scylla. Can provide specific version after a colon e.g: `https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-2021.1.list:2021.1.18`

**default:** N/A

**type:** str (appendable)


## **scylla_version** / SCT_SCYLLA_VERSION

Version of scylla to install, ex. '2.3.1'<br>Automatically lookup AMIs and repo links for formal versions.<br>WARNING: can't be used together with [`scylla_repo`](#scylla_repo) or [`ami_id_db_scylla`](aws-backend.md#ami_id_db_scylla)

**default:** N/A

**type:** str

**backend overrides:**
- `6.2.3`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **server_encrypt** / SCT_SERVER_ENCRYPT

when enable scylla will use encryption on the server side

**default:** False

**type:** bool


## **server_encrypt_mtls** / SCT_SERVER_ENCRYPT_MTLS

when enabled scylla will enforce mutual authentication when node-to-node encryption is enabled

**default:** False

**type:** bool


## **service_level_shares** / SCT_SERVICE_LEVEL_SHARES

List if service level shares - how many server levels to create and test. Uses in SLA test. list of int, like: [100, 200]

**default:** [1000]

**type:** list


## **unified_package** / SCT_UNIFIED_PACKAGE

Url to the unified package of scylla version to install scylla

**default:** N/A

**type:** str (appendable)


## **update_db_packages** / SCT_UPDATE_DB_PACKAGES

A local directory of rpms to install a custom version on top of<br>the scylla installed (or from repo or from ami)

**default:** N/A

**type:** str (appendable)


## **use_ldap** / SCT_USE_LDAP

When defined true, LDAP is going to be used.

**default:** N/A

**type:** bool


## **use_ldap_authentication** / SCT_USE_LDAP_AUTHENTICATION

Authenticate Scylla users against LDAP: starts an LDAP container and sets scylla.yaml to use it for authentication (who you are).

**default:** N/A

**type:** bool


## **use_ldap_authorization** / SCT_USE_LDAP_AUTHORIZATION

Authorize Scylla users through LDAP group membership: starts an LDAP container and sets scylla.yaml to use it for authorization (what you may do).

**default:** N/A

**type:** bool


## **use_preinstalled_scylla** / SCT_USE_PREINSTALLED_SCYLLA

Don't install/update ScyllaDB on DB nodes

**default:** False

**type:** bool

**backend overrides:**
- `True`: aws, gce, azure, oci, docker, aws-siren, gce-siren, k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **user_data_format_version** / SCT_USER_DATA_FORMAT_VERSION

user-data format version to send to the DB node images. Defaults to whatever the image is tagged with; set it only to override that.

**default:** N/A

**type:** str
