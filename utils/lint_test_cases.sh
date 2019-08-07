#! /bin/bash

for f in `find ./test-cases/ \\( -iname "*.yaml" ! -iname "*multi-dc.yaml" ! -iname *multiDC*.yaml ! -iname *multiple-dc*.yaml ! -iname *rolling* \\)` ; do
    echo "---- linting: $f -----"
    RES=$( script --flush --quiet --return /tmp/test-case.txt --command "SCT_AMI_ID_DB_SCYLLA=abc ./docker/env/hydra.sh conf $f" )
    if [[ "$?" == "1" ]]; then
        cat /tmp/test-case.txt
        exit 1;
    fi
done

for f in `find ./test-cases/ \\( -iname *multi-dc.yaml -or -iname *multiDC*.yaml -or -iname *multiple-dc*.yaml -or -iname *rolling* \\)`; do
    echo "---- linting: $f -----"
    RES=$( script --flush --quiet --return /tmp/test-case.txt --command "SCT_SCYLLA_REPO=abc ./docker/env/hydra.sh conf --backend gce $f" )
    if [[ "$?" == "1" ]]; then
        cat /tmp/test-case.txt
        exit 1;
    fi
done
