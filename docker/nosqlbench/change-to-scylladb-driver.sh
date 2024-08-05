#!/bin/bash
# -*- coding: utf-8 -*-

SCYLLA_JAVA_DRIVER_VERSION=${1:-"4.18.0.1"}
POM_FILE=${2:-"./mvn-defaults/pom.xml"}

POM_FILE_PATH=$(realpath "$POM_FILE" || exit 1)

replace_in_pom() {
    xmlstarlet ed --inplace -N x="http://maven.apache.org/POM/4.0.0" \
        -u "//x:project/x:dependencyManagement/x:dependencies/x:dependency[x:groupId='org.apache.cassandra']/x:groupId" \
        -v "com.scylladb" \
        pom.xml

    xmlstarlet ed --inplace -N x="http://maven.apache.org/POM/4.0.0" \
        -u "//x:project/x:dependencyManagement/x:dependencies/x:dependency[x:groupId='com.scylladb']/x:version" \
        -v "$SCYLLA_JAVA_DRIVER_VERSION" \
        pom.xml

    diff "$POM_FILE_PATH" "$POM_FILE_PATH.bak"
}

validate_pom() {
    if !  xmlstarlet val -e pom.xml; then
        echo "Invalid pom.xml after replacing the dependencies in $POM_FILE_PATH... REVERTING CHANGES"
        mv "$POM_FILE_PATH.bak" "$POM_FILE_PATH"
        return;
    fi

    if ! mvn validate >> /dev/null;  then
        echo "The specified dependencies have not been replaced in $POM_FILE_PATH... REVERTING CHANGES"
        mv "$POM_FILE_PATH.bak" "$POM_FILE_PATH"
        return;
    fi

    echo "The specified dependencies have been replaced in $POM_FILE_PATH"
    rm -f "$POM_FILE_PATH.bak"
}

_main() {
    cp "$POM_FILE_PATH" "$POM_FILE_PATH.bak" || exit 1

    replace_in_pom

    validate_pom
}


_main