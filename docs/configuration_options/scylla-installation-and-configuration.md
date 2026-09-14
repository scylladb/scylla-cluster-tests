# Scylla installation and configuration

[← All configuration options](../configuration_options.md)

Which Scylla to install and how it is configured: repos, versions, distro,
`scylla.yaml`/command-line options, experimental features, authentication and encryption.

**44 options.**


<a id="append_scylla_args"></a>

## **append_scylla_args** / SCT_APPEND_SCYLLA_ARGS

More arguments to append to scylla command line

**default:** --blocked-reactor-notify-ms 25 --abort-on-lsa-bad-alloc 1 --abort-on-seastar-bad-alloc --abort-on-internal-error 1 --abort-on-ebadf 1 --enable-sstable-key-validation 1

**type:** str (appendable)

**backend overrides:**
- `--abort-on-lsa-bad-alloc 1 --abort-on-internal-error 1 --abort-on-ebadf 1 --enable-sstable-key-validation 1`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


<a id="append_scylla_node_exporter_args"></a>

## **append_scylla_node_exporter_args** / SCT_APPEND_SCYLLA_NODE_EXPORTER_ARGS

More arguments to append to scylla-node-exporter command line

**default:** N/A

**type:** str (appendable)


<a id="append_scylla_setup_args"></a>

## **append_scylla_setup_args** / SCT_APPEND_SCYLLA_SETUP_ARGS

More arguments to append to scylla_setup command line

**default:** N/A

**type:** str (appendable)


<a id="append_scylla_yaml"></a>

## **append_scylla_yaml** / SCT_APPEND_SCYLLA_YAML

More configuration to append to /etc/scylla/scylla.yaml

**default:** {'rf_rack_valid_keyspaces': True}

**type:** dict | str | pydantic.main.BaseModel


<a id="assert_linux_distro_features"></a>

## **assert_linux_distro_features** / SCT_ASSERT_LINUX_DISTRO_FEATURES

List of distro features relevant to SCT test. Example: 'fips'.<br>This is used to assert that the distro features are supported by the scylla version being tested.<br>If the feature is not supported, the test will fail.

**default:** []

**type:** str | list[str] → list[str] (appendable)


<a id="authenticator"></a>

## **authenticator** / SCT_AUTHENTICATOR

which authenticator scylla will use AllowAllAuthenticator/PasswordAuthenticator

**default:** N/A

**type:** Literal['PasswordAuthenticator', 'AllowAllAuthenticator', 'com.scylladb.auth.SaslauthdAuthenticator']


<a id="authenticator_password"></a>

## **authenticator_password** / SCT_AUTHENTICATOR_PASSWORD

the password if PasswordAuthenticator is used

**default:** N/A

**type:** str (appendable)


<a id="authenticator_user"></a>

## **authenticator_user** / SCT_AUTHENTICATOR_USER

the username if PasswordAuthenticator is used

**default:** N/A

**type:** str (appendable)


<a id="authorizer"></a>

## **authorizer** / SCT_AUTHORIZER

which authorizer scylla will use AllowAllAuthorizer/CassandraAuthorizer

**default:** N/A

**type:** Literal['AllowAllAuthorizer', 'CassandraAuthorizer']


<a id="client_encrypt"></a>

## **client_encrypt** / SCT_CLIENT_ENCRYPT

when enable scylla will use encryption on the client side

**default:** False

**type:** bool


<a id="client_encrypt_mtls"></a>

## **client_encrypt_mtls** / SCT_CLIENT_ENCRYPT_MTLS

when enabled scylla will enforce mutual authentication when client-to-node encryption is enabled

**default:** False

**type:** bool


<a id="db_type"></a>

## **db_type** / SCT_DB_TYPE

Db type to install into db nodes, scylla/cassandra

**default:** scylla

**type:** str (appendable)


<a id="enable_kms_key_rotation"></a>

## **enable_kms_key_rotation** / SCT_ENABLE_KMS_KEY_ROTATION

Allows to disable KMS keys rotation. Applicable to AWS, GCP, and Azure backends.

**default:** True

**type:** bool


<a id="endpoint_snitch"></a>

## **endpoint_snitch** / SCT_ENDPOINT_SNITCH

The snitch class scylla would use<br><br>'GossipingPropertyFileSnitch' - default<br>'Ec2MultiRegionSnitch' - default on aws backend<br>'GoogleCloudSnitch'

**default:** N/A

**type:** str (appendable)


<a id="enterprise_disable_kms"></a>

## **enterprise_disable_kms** / SCT_ENTERPRISE_DISABLE_KMS

An escape hatch to disable KMS for enterprise run, when needed. We enable KMS by default since if we use Scylla 2023.1.3 and up

**default:** False

**type:** bool


<a id="experimental_features"></a>

## **experimental_features** / SCT_EXPERIMENTAL_FEATURES

Scylla experimental features to enable in scylla.yaml, as a list of feature names (e.g. 'udf', 'alternator-streams').

**default:** []

**type:** str | list[str] → list[str] (appendable)


<a id="hinted_handoff"></a>

## **hinted_handoff** / SCT_HINTED_HANDOFF

when enable or disable scylla hinted handoff (enabled/disabled)

**default:** disabled

**type:** str (appendable)


<a id="install_mode"></a>

## **install_mode** / SCT_INSTALL_MODE

Scylla install mode, repo/offline/web

**default:** repo

**type:** str


<a id="internode_compression"></a>

## **internode_compression** / SCT_INTERNODE_COMPRESSION

Scylla [`internode_compression`](#internode_compression) in scylla.yaml: which inter-node traffic to compress -- 'all', 'dc' (between datacenters only) or 'none'.

**default:** N/A

**type:** str (appendable)


<a id="internode_encryption"></a>

## **internode_encryption** / SCT_INTERNODE_ENCRYPTION

Scylla sub option of server_encryption_options: [`internode_encryption`](#internode_encryption).

**default:** all

**type:** str (appendable)


<a id="jmx_heap_memory"></a>

## **jmx_heap_memory** / SCT_JMX_HEAP_MEMORY

The total size of the memory allocated to JMX. Values in MB, so for 1GB enter 1024(MB).

**default:** N/A

**type:** int


<a id="kms_key_rotation_interval"></a>

## **kms_key_rotation_interval** / SCT_KMS_KEY_ROTATION_INTERVAL

The time interval in minutes which gets waited before the KMS key rotation happens. Applied when the AWS KMS service is configured to be used.

**default:** N/A

**type:** int

**backend overrides:**
- `60`: aws, gce, azure, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


<a id="ldap_server_type"></a>

## **ldap_server_type** / SCT_LDAP_SERVER_TYPE

This option indicates which server is going to be used for LDAP operations. [openldap, ms_ad]

**default:** N/A

**type:** str (appendable)


<a id="nonroot_offline_install"></a>

## **nonroot_offline_install** / SCT_NONROOT_OFFLINE_INSTALL

Install Scylla without required root privilege

**default:** N/A

**type:** bool


<a id="peer_verification"></a>

## **peer_verification** / SCT_PEER_VERIFICATION

enable peer verification for encrypted communication

**default:** True

**type:** bool


<a id="prepare_saslauthd"></a>

## **prepare_saslauthd** / SCT_PREPARE_SASLAUTHD

When defined true, will install and start saslauthd service

**default:** N/A

**type:** bool


<a id="scylla_apt_keys"></a>

## **scylla_apt_keys** / SCT_SCYLLA_APT_KEYS

APT keys for ScyllaDB repos

**default:** ['17723034C56D4B19', '5E08FBD8B5D6EC9C', 'D0A112E067426AB2', '491C93B9DE7496A7', 'A43E06657BAC99E3', 'C503C686B007F39E']

**type:** str | list[str] → list[str] (appendable)


<a id="scylla_d_overrides_files"></a>

## **scylla_d_overrides_files** / SCT_SCYLLA_D_OVERRIDES_FILES

list of files that should upload to /etc/scylla.d/ directory to override scylla config files

**default:** []

**type:** str | list[str] → list[str] (appendable)


<a id="scylla_encryption_options"></a>

## **scylla_encryption_options** / SCT_SCYLLA_ENCRYPTION_OPTIONS

options will be used for enable encryption at-rest for tables

**default:** N/A

**type:** str (appendable)


<a id="scylla_linux_distro"></a>

## **scylla_linux_distro** / SCT_SCYLLA_LINUX_DISTRO

Distro and family for the DB node image, e.g. 'ubuntu-jammy' or 'debian-bookworm'.

**default:** ubuntu-focal

**type:** str

**backend overrides:**
- `centos`: docker


<a id="scylla_linux_distro_loader"></a>

## **scylla_linux_distro_loader** / SCT_SCYLLA_LINUX_DISTRO_LOADER

Distro and family for the loader node image. Independent of the DB nodes, so loaders can run a different distro.

**default:** ubuntu-jammy

**type:** str


<a id="scylla_network_config"></a>

## **scylla_network_config** / SCT_SCYLLA_NETWORK_CONFIG

Configure Scylla networking with single or multiple NIC/IP combinations.<br>It must be defined for listen_address and rpc_address. For each address mandatory parameters are:<br>- address: listen_address/rpc_address/broadcast_rpc_address/broadcast_address/test_communication<br>- ip_type: ipv4 or ipv6<br>- public: false or true<br>- nic: number of NIC. 0, 1<br>Supported for the AWS, GCE, OCI and Azure backends

**default:** N/A

**type:** list

**backend overrides:**
- `[{'address': 'listen_address', 'listen_all': False, 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}, {'address': 'rpc_address', 'listen_all': False, 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}, {'address': 'broadcast_rpc_address', 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}, {'address': 'broadcast_address', 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}, {'address': 'test_communication', 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}]`: aws, gce, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


<a id="scylla_repo"></a>

## **scylla_repo** / SCT_SCYLLA_REPO

Url to the repo of scylla version to install scylla. Can provide specific version after a colon e.g: `https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-2021.1.list:2021.1.18`

**default:** N/A

**type:** str (appendable)


<a id="scylla_version"></a>

## **scylla_version** / SCT_SCYLLA_VERSION

Version of scylla to install, ex. '2.3.1'<br>Automatically lookup AMIs and repo links for formal versions.<br>WARNING: can't be used together with [`scylla_repo`](#scylla_repo) or [`ami_id_db_scylla`](aws-backend.md#ami_id_db_scylla)

**default:** N/A

**type:** str

**backend overrides:**
- `6.2.3`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


<a id="server_encrypt"></a>

## **server_encrypt** / SCT_SERVER_ENCRYPT

when enable scylla will use encryption on the server side

**default:** False

**type:** bool


<a id="server_encrypt_mtls"></a>

## **server_encrypt_mtls** / SCT_SERVER_ENCRYPT_MTLS

when enabled scylla will enforce mutual authentication when node-to-node encryption is enabled

**default:** False

**type:** bool


<a id="service_level_shares"></a>

## **service_level_shares** / SCT_SERVICE_LEVEL_SHARES

List if service level shares - how many server levels to create and test. Uses in SLA test. list of int, like: [100, 200]

**default:** [1000]

**type:** list


<a id="unified_package"></a>

## **unified_package** / SCT_UNIFIED_PACKAGE

Url to the unified package of scylla version to install scylla

**default:** N/A

**type:** str (appendable)


<a id="update_db_packages"></a>

## **update_db_packages** / SCT_UPDATE_DB_PACKAGES

A local directory of rpms to install a custom version on top of<br>the scylla installed (or from repo or from ami)

**default:** N/A

**type:** str (appendable)


<a id="use_ldap"></a>

## **use_ldap** / SCT_USE_LDAP

When defined true, LDAP is going to be used.

**default:** N/A

**type:** bool


<a id="use_ldap_authentication"></a>

## **use_ldap_authentication** / SCT_USE_LDAP_AUTHENTICATION

Authenticate Scylla users against LDAP: starts an LDAP container and sets scylla.yaml to use it for authentication (who you are).

**default:** N/A

**type:** bool


<a id="use_ldap_authorization"></a>

## **use_ldap_authorization** / SCT_USE_LDAP_AUTHORIZATION

Authorize Scylla users through LDAP group membership: starts an LDAP container and sets scylla.yaml to use it for authorization (what you may do).

**default:** N/A

**type:** bool


<a id="use_preinstalled_scylla"></a>

## **use_preinstalled_scylla** / SCT_USE_PREINSTALLED_SCYLLA

Don't install/update ScyllaDB on DB nodes

**default:** False

**type:** bool

**backend overrides:**
- `True`: aws, gce, azure, oci, docker, aws-siren, gce-siren, k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


<a id="user_data_format_version"></a>

## **user_data_format_version** / SCT_USER_DATA_FORMAT_VERSION

user-data format version to send to the DB node images. Defaults to whatever the image is tagged with; set it only to override that.

**default:** N/A

**type:** str
