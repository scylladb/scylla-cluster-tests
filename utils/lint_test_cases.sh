#! /bin/bash

for f in `find ./test-cases/ -type f \\( -iname "*.yaml" ! -iname "*multi-dc.yaml" ! -iname *multiDC*.yaml ! -iname *multiple-dc*.yaml ! -iname *rolling* ! -iregex .*docker.* ! -iregex .*artifacts.* !  -iregex .*private-repo.* ! -iregex .*ics/long.* ! -iregex .*scylla-operator/.* ! -iregex .*gce.* ! -iregex .*jepsen.* \\)` ; do
    echo "---- linting: $f -----"
    RES=$( script --flush --quiet --return /tmp/test-case.txt --command "SCT_SCYLLA_VERSION=4.0.0 python3 ./sct.py conf $f" )
    if [[ "$?" == "1" ]]; then
        cat /tmp/test-case.txt
        exit 1;
    fi
done

for f in `find ./test-cases/ -type f \\( -iname *multi-dc.yaml -or -iname *multiDC*.yaml -or -iname *multiple-dc*.yaml -or -iname *rolling* -or -iregex .*artifacts.*yaml -or -iregex .*private-repo.*\.yaml -or -iregex .*gce.* -or -iregex .*jepsen.* \\) ! -name docker.yaml`; do
    echo "---- linting: $f -----"
    RES=$( script --flush --quiet --return /tmp/test-case.txt --command "SCT_SCYLLA_REPO=http://downloads.scylladb.com.s3.amazonaws.com/rpm/centos/scylla-2019.1.repo python3 ./sct.py conf --backend gce $f" )
    if [[ "$?" == "1" ]]; then
        cat /tmp/test-case.txt
        exit 1;
    fi
done

for f in `find ./test-cases/ -type f \\( -iname "*.yaml" -iregex .*scylla-operator/.* \\)` ; do
    echo "---- linting: $f -----"
    RES=$( script --flush --quiet --return /tmp/test-case.txt --command "SCT_SCYLLA_VERSION=4.0.0 python3 ./sct.py conf -b 'k8s-gce-minikube' $f" )
    if [[ "$?" == "1" ]]; then
        cat /tmp/test-case.txt
        exit 1;
    fi
done
