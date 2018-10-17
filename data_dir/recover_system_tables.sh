#!/bin/bash

DIR='/var/lib/scylla/data/system';
for i in `sudo ls $DIR`;
do
    sudo test -e $DIR/$i/snapshots/$1 && sudo find $DIR/$i/snapshots/$1 -type f -exec sudo /bin/cp {} $DIR/$i/ \;
done

# Some new table may not exist in old version, `test -e` might failed (return -1),
# it might be return to this script.
true
