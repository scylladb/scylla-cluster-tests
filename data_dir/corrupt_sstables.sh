#! /bin/bash

DATA_DIR='/var/lib/scylla/data/keyspace1'  # data files path
N=5                                        # number of files to corrupt

files=`find $DATA_DIR/* \( -name "*Data.db" -o -name "*Index.db" \) | head -$N`
echo "Files chosen to corrupt: $files"

for f in $files
do
	echo "Corrupt file $f"
	for i in {1..3}
	do
	    offset=$(( $RANDOM % 100 + 1))
	    text=`cat /dev/urandom | tr -dc 'a-zA-Z0-9'|fold -w 5 | head -1`
        sed -i ${offset}i${text} $f
        echo "Command exit code is" $?
    done
done
