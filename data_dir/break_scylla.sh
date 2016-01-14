#!/bin/bash

DELETE_COUNT=5

ls /var/lib/scylla|sort -R |tail -$DELETE_COUNT |while read file; do
    rm -rf $file
done
