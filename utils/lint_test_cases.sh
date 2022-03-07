#! /bin/bash

OUT=0
SCT_SCYLLA_VERSION=4.4.1 ./sct.py lint-yamls -i '.yaml' -e 'multi-dc,multiDC,multidc,multiple-dc,rolling,docker,artifacts,private-repo,ics/long,scylla-operator,gce,jepsen'
OUT=$(($OUT + $?))

SCT_SCYLLA_REPO='http://downloads.scylladb.com.s3.amazonaws.com/rpm/centos/scylla-2021.1.repo' ./sct.py lint-yamls -b gce -i 'multi-dc,multiDC,multidc,multiple-dc,rolling,artifacts,private-repo,gce,jepsen' -e 'docker'
OUT=$(($OUT + $?))

SCT_SCYLLA_VERSION=4.4.1 ./sct.py lint-yamls -b k8s-local-kind -i 'scylla-operator.*\.yaml'
OUT=$(($OUT + $?))

exit $OUT
