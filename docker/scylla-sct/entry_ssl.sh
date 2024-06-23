#!/bin/bash

if [[ ! $(grep 'skip_wait_for_gossip_to_settle' /etc/scylla/scylla.yaml) ]]; then echo "skip_wait_for_gossip_to_settle: 0" >> /etc/scylla/scylla.yaml ; fi

cat <<EOM >> /etc/scylla/scylla.yaml

alternator_enforce_authorization: true
authenticator: 'PasswordAuthenticator'
authenticator_user: cassandra
authenticator_password: cassandra
authorizer: 'CassandraAuthorizer'

client_encryption_options:
  certificate: /etc/scylla/ssl_conf/client/test.crt
  enabled: true
<<<<<<< HEAD
  keyfile: /etc/scylla/ssl_conf/client/test.key
  truststore: /etc/scylla/ssl_conf/client/catest.pem
=======
  keyfile: /etc/scylla/ssl_conf/client-facing.key
  truststore: /etc/scylla/ssl_conf/ca.pem

<<<<<<< HEAD
enable_tablets: false
>>>>>>> 22d60115 (fix(integration-tests): enable auth by default)
=======
>>>>>>> 90e53244 (fix(kafka_cluster): add support for auth)
EOM

sed -e '/enable_tablets:.*/s/true/false/g' -i /etc/scylla/scylla.yaml

/docker-entrypoint.py $*
