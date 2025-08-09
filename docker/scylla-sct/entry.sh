#!/bin/bash

set -x

if [[ ! $(grep 'skip_wait_for_gossip_to_settle' /etc/scylla/scylla.yaml) ]]; then echo "skip_wait_for_gossip_to_settle: 0" >> /etc/scylla/scylla.yaml ; fi

# TODO: Remove conditional setting of auth parameters when authentication is implemented in vector-store
if [[ "${VECTOR_SEARCH_TEST:-}" != "true" ]]; then
cat <<EOM >> /etc/scylla/scylla.yaml

alternator_enforce_authorization: true
authenticator: 'PasswordAuthenticator'
authenticator_user: cassandra
authenticator_password: cassandra
authorizer: 'CassandraAuthorizer'
EOM
fi

sed -e '/enable_tablets:.*/s/true/false/g' -i /etc/scylla/scylla.yaml
sed -e '/tablets_mode_for_new_keyspaces:.*/s/enabled/disabled/g' -i /etc/scylla/scylla.yaml

/docker-entrypoint.py $*
