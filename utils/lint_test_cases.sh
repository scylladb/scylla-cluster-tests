#! /bin/bash

OUT=0
SCT_SCYLLA_VERSION=4.4.1 ./sct.py lint-yamls -i '.yaml' -e 'multi-dc,multiDC,multidc,multiple-dc,rolling,docker,artifacts,private-repo,ics/long,scylla-operator,gce,jepsen,repair-based-operations'
OUT=$(($OUT + $?))

SCT_GCE_IMAGE_DB=image SCT_SCYLLA_REPO='http://downloads.scylladb.com.s3.amazonaws.com/rpm/centos/scylla-2021.1.repo' ./sct.py lint-yamls -b gce -i 'rolling,artifacts,private-repo,gce,jepsen' -e 'multi-dc,docker,azure'
OUT=$(($OUT + $?))

echo "multi dc yamls with 2 regions"
SCT_GCE_IMAGE_DB=image SCT_REGION_NAME="us-east1 us-west1" SCT_SCYLLA_REPO='http://downloads.scylladb.com.s3.amazonaws.com/rpm/centos/scylla-2021.1.repo' ./sct.py lint-yamls -b gce -i 'multi-dc-rolling,3h-multidc,multidc-4h,topology-changes,snitch-multi-dc,manager-regression-multiDC' -e 'docker,azure,shutdown'
OUT=$(($OUT + $?))

echo "multi dc yamls with 3 regions"
SCT_GCE_IMAGE_DB=image SCT_REGION_NAME="us-east1 us-west1 eu-north1" SCT_SCYLLA_REPO='http://downloads.scylladb.com.s3.amazonaws.com/rpm/centos/scylla-2021.1.repo' ./sct.py lint-yamls -b gce -i '24h-multidc,large-cluster,counters-multidc' -e 'docker,azure,shutdown'
OUT=$(($OUT + $?))

SCT_SCYLLA_VERSION=4.4.1 ./sct.py lint-yamls -b k8s-local-kind -i 'scylla-operator.*\.yaml'
OUT=$(($OUT + $?))

exit $OUT
