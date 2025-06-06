#!/bin/bash

if [[ ! $(grep 'skip_wait_for_gossip_to_settle' /etc/scylla/scylla.yaml) ]]; then echo "skip_wait_for_gossip_to_settle: 0" >> /etc/scylla/scylla.yaml ; fi

cat <<EOM >> /etc/scylla/scylla.yaml

alternator_enforce_authorization: true
authenticator: 'PasswordAuthenticator'
authenticator_user: cassandra
authenticator_password: cassandra
authorizer: 'CassandraAuthorizer'

client_encryption_options:
  certificate: /etc/scylla/ssl_conf/client-facing.crt
  enabled: true
  keyfile: /etc/scylla/ssl_conf/client-facing.key
  truststore: /etc/scylla/ssl_conf/ca.pem

EOM

sed -e '/enable_tablets:.*/s/true/false/g' -i /etc/scylla/scylla.yaml
sed -e '/tablets_mode_for_new_keyspaces:.*/s/enabled/disabled/g' -i /etc/scylla/scylla.yaml

/docker-entrypoint.py $*
