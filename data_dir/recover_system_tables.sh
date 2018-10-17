#!/bin/bash

DIR='/var/lib/scylla/data/system';
for i in `sudo ls $DIR`;
do
    sudo test -e $DIR/$i/snapshots/$1 && sudo find $DIR/$i/snapshots/$1 -type f -exec sudo /bin/cp {} $DIR/$i/ \;
done
true
