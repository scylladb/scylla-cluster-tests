#! /bin/bash

SCT_SCYLLA_VERSION=4.2.0 ./sct.py lint-yamls -i '.yaml' -e 'multi-dc.yaml,multiDC,multidc,multiple-dc,rolling,docker,artifacts,private-repo,ics/long,scylla-operator,gce,jepsen'

SCT_SCYLLA_REPO='http://downloads.scylladb.com.s3.amazonaws.com/rpm/centos/scylla-2019.1.repo' ./sct.py lint-yamls -b gce -i 'multi-dc.yaml,multiDC,multidc,multiple-dc,rolling,artifacts,private-repo,gce,jepsen' -e 'docker'

SCT_SCYLLA_VERSION=4.2.0 ./sct.py lint-yamls -b k8s-gce-minikube -i 'scylla-operator.*\.yaml'
