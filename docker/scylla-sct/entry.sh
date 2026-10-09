#!/bin/bash

set -x

if [[ ! $(grep 'skip_wait_for_gossip_to_settle' /etc/scylla/scylla.yaml) ]]; then echo "skip_wait_for_gossip_to_settle: 0" >> /etc/scylla/scylla.yaml ; fi

# Vector search tests enable auth only with VECTOR_SEARCH_AUTH=true, because vector-store then needs its own ScyllaDB user.
if [[ "${VECTOR_SEARCH_TEST:-}" != "true" || "${VECTOR_SEARCH_AUTH:-}" == "true" ]]; then
cat <<EOM >> /etc/scylla/scylla.yaml

alternator_enforce_authorization: true
authenticator: 'PasswordAuthenticator'
authenticator_user: cassandra
authenticator_password: cassandra
authorizer: 'CassandraAuthorizer'
EOM
fi

# Scylla 2026.2 and later create no default superuser, so the vector search auth tests set it.
if [[ "${VECTOR_SEARCH_AUTH:-}" == "true" ]]; then
cat <<'EOM' >> /etc/scylla/scylla.yaml
auth_superuser_name: cassandra
auth_superuser_salted_password: "$6$x7IFjiX5VCpvNiFk$2IfjTvSyGL7zerpV.wbY7mJjaRCrJ/68dtT3UpT.sSmNYz1bPjtn3mH.kJKFvaZ2T4SbVeBijjmwGjcb83LlV/"
EOM
fi

# vector indexes require tablets
if [[ "${VECTOR_SEARCH_TEST:-}" != "true" ]]; then
sed -e '/enable_tablets:.*/s/true/false/g' -i /etc/scylla/scylla.yaml
sed -e '/tablets_mode_for_new_keyspaces:.*/s/enabled/disabled/g' -i /etc/scylla/scylla.yaml
fi

/docker-entrypoint.py $*
