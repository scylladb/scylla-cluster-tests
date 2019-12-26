#!/bin/bash

for DIR in '/var/lib/scylla/data/system' '/var/lib/scylla/data/system_schema'; do
    for i in `sudo ls $DIR`;
    do
        sudo test -e $DIR/$i/snapshots/$1 && (sudo rm -f $DIR/$i/* || true) && sudo find $DIR/$i/snapshots/$1 -type f -exec sudo -u scylla /bin/cp {} $DIR/$i/ \;
    done
done


# Some new table may not exist in old version, `test -e` might failed (return -1),
# it might be return to this script.
true
