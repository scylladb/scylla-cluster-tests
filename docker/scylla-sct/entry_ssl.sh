#!/bin/bash

if [[ ! $(grep 'skip_wait_for_gossip_to_settle' /etc/scylla/scylla.yaml) ]]; then echo "skip_wait_for_gossip_to_settle: 0" >> /etc/scylla/scylla.yaml ; fi

cat <<EOM >> /etc/scylla/scylla.yaml

client_encryption_options:
  certificate: /etc/scylla/ssl_conf/client/test.crt
  enabled: true
  keyfile: /etc/scylla/ssl_conf/client/test.key
  truststore: /etc/scylla/ssl_conf/client/catest.pem
EOM

/docker-entrypoint.py $*
