#! /bin/bash

for f in `find ./test-cases/ \\( -iname "*.yaml" ! -iname "*multi-dc.yaml" ! -iname *multiDC*.yaml ! -iname *multiple-dc*.yaml ! -iname *rolling* ! -iregex .*docker.* ! -iregex .*artifacts.* !  -iregex .*private-repo.* ! -iregex .*ics/long.* \\)` ; do
    echo "---- linting: $f -----"
    RES=$( script --flush --quiet --return /tmp/test-case.txt --command "SCT_SCYLLA_VERSION=4.0.0 python3 ./sct.py conf $f" )
    if [[ "$?" == "1" ]]; then
        cat /tmp/test-case.txt
        exit 1;
    fi
done

for f in `find ./test-cases/ \\( -iname *multi-dc.yaml -or -iname *multiDC*.yaml -or -iname *multiple-dc*.yaml -or -iname *rolling* -or -iregex .*artifacts.*yaml -or -iregex .*private-repo.*\.yaml \\) ! -name docker.yaml`; do
    echo "---- linting: $f -----"
    RES=$( script --flush --quiet --return /tmp/test-case.txt --command "SCT_SCYLLA_REPO=http://repositories.scylladb.com/scylla/repo/qa-test/centos/scylladb-2019.1.repo python3 ./sct.py conf --backend gce $f" )
    if [[ "$?" == "1" ]]; then
        cat /tmp/test-case.txt
        exit 1;
    fi
done
