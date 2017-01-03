#!/bin/bash

CASSANDRA_CONF_FILE="/etc/cassandra/cassandra.yaml"

sed s/"compaction_throughput_mb_per_sec: 16"/"compaction_throughput_mb_per_sec: 0"/g -i ${CASSANDRA_CONF_FILE}
echo "stream_throughput_outbound_megabits_per_sec: 2000" >>${CASSANDRA_CONF_FILE}
echo "inter_dc_stream_throughput_outbound_megabits_per_sec: 2000" >> ${CASSANDRA_CONF_FILE}

# if we want cassandra to start we must make it ignore these because scylla
# prepared then differently
echo JVM_OPTS=\"\$JVM_OPTS -Dcassandra.ignore_dc=true\" >> /etc/cassandra/cassandra-env.sh
echo JVM_OPTS=\"\$JVM_OPTS -Dcassandra.ignore_rack=true\" >> /etc/cassandra/cassandra-env.sh
