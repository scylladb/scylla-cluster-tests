#!/bin/bash
# -*- coding: utf-8 -*-

SCYLLA_JAVA_DRIVER_VERSION=${1:-"4.18.0.1"}

replace_in_pom() {
    local pom_file=$1

    cp "$pom_file" "$pom_file.bak" || exit 1

    xmlstarlet ed --inplace -N x="http://maven.apache.org/POM/4.0.0" \
        -u "//x:project/x:dependencyManagement/x:dependencies/x:dependency[x:groupId='org.apache.cassandra']/x:groupId" \
        -v "com.scylladb" \
        "$pom_file"

    xmlstarlet ed --inplace -N x="http://maven.apache.org/POM/4.0.0" \
        -u "//x:project/x:dependencyManagement/x:dependencies/x:dependency[x:groupId='com.scylladb']/x:version" \
        -v "$SCYLLA_JAVA_DRIVER_VERSION" \
        "$pom_file"

    xmlstarlet ed --inplace -N x="http://maven.apache.org/POM/4.0.0" \
        -u "//x:project/x:dependencies/x:dependency[x:groupId='org.apache.cassandra']/x:groupId" \
        -v "com.scylladb" \
        "$pom_file"

    xmlstarlet ed --inplace -N x="http://maven.apache.org/POM/4.0.0" \
        -u "//x:project/x:dependencies/x:dependency[x:groupId='com.scylladb']/x:version" \
        -v "$SCYLLA_JAVA_DRIVER_VERSION" \
        "$pom_file"

    diff "$pom_file" "$pom_file.bak"
}

validate_pom() {
    local pom_file=$1

    if !  xmlstarlet val -e pom.xml; then
        echo "Invalid pom.xml after replacing the dependencies in $pom_file... REVERTING CHANGES"
        mv "$pom_file.bak" "$pom_file"
        return 1;
    fi

    echo "The specified dependencies have been replaced in $pom_file"
    rm -f "$pom_file.bak"

    return 0;
}

if [ "$#" -lt 2 ]; then
    default_paths=("$(realpath "mvn-defaults/pom.xml" || exit 1)" "$(realpath "./nb-adapters/adapter-cqld4/pom.xml" || exit 1)")
    set -- "${default_paths[@]}"
fi

for file in "${@:1}"; do
    pom_file=$(realpath "$file" || exit 1)

    replace_in_pom "$pom_file"

    if ! validate_pom "$pom_file" ; then
        exit 1
    fi
done
