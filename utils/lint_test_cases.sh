#! /bin/bash

OUT=0
SCT_AMI_ID_DB_SCYLLA=ami-1234 ./sct.py lint-yamls -i '.yaml' -e 'azure,multi-dc,multiDC,multidc,multiple-dc,3dcs,5dcs,rolling,docker,artifacts,private-repo,ics/long,scylla-operator,gce,custom-d3,jepsen,repair-based-operations,add-new-dc,baremetal'
OUT=$(($OUT + $?))

SCT_AZURE_IMAGE_DB=image SCT_AZURE_REGION_NAME="eastus" ./sct.py lint-yamls --backend azure -i azure
OUT=$(($OUT + $?))

SCT_GCE_IMAGE_DB=image SCT_SCYLLA_REPO='http://downloads.scylladb.com.s3.amazonaws.com/rpm/centos/scylla-2021.1.repo' ./sct.py lint-yamls -b gce -i 'rolling,artifacts,private-repo,gce,custom-d3,jepsen' -e 'multi-dc,multiDC,docker,azure,5dcs'
OUT=$(($OUT + $?))

echo "multi dc yamls with 2 regions"
SCT_GCE_IMAGE_DB=image SCT_GCE_DATACENTER="us-east1 us-west1" SCT_SCYLLA_REPO='http://downloads.scylladb.com.s3.amazonaws.com/rpm/centos/scylla-2021.1.repo' ./sct.py lint-yamls -b gce -i 'multi-dc-rolling,3h-multidc,multidc-4h,multidc-parallel,snitch-multi-dc,manager-regression-multiDC' -e 'docker,azure,shutdown'
OUT=$(($OUT + $?))

echo "multi dc yamls with 3 regions"
SCT_GCE_IMAGE_DB=image SCT_GCE_DATACENTER="us-east1 us-west1 eu-north1" SCT_SCYLLA_REPO='http://downloads.scylladb.com.s3.amazonaws.com/rpm/centos/scylla-2021.1.repo' ./sct.py lint-yamls -b gce -i '3dcs,24h-multidc,large-cluster,counters-multidc,cdc-8h-multi-dc' -e 'docker,azure,shutdown'
OUT=$(($OUT + $?))

echo "multi dc yamls with 5 regions"
SCT_AMI_ID_DB_SCYLLA="ami-1 ami-2 ami-3 ami-4 ami-5" SCT_REGION_NAME="eu-central-1 eu-north-1 eu-west-1 eu-west-2 us-east-1" SCT_SCYLLA_REPO='http://downloads.scylladb.com.s3.amazonaws.com/rpm/centos/scylla-2021.1.repo' ./sct.py lint-yamls -b aws -i '5dcs' -e 'docker,azure,shutdown'
OUT=$(($OUT + $?))


echo "baremetal"
SCT_DB_NODES_PUBLIC_IP='["127.0.0.1", "127.0.0.2"]' SCT_SCYLLA_REPO='http://downloads.scylladb.com.s3.amazonaws.com/rpm/centos/scylla-2021.1.repo' ./sct.py lint-yamls -b baremetal  -i 'baremetal'
OUT=$(($OUT + $?))


SCT_AMI_ID_DB_SCYLLA=ami-1234 ./sct.py lint-yamls -b k8s-local-kind -i 'scylla-operator.*\.yaml'
OUT=$(($OUT + $?))

exit $OUT
