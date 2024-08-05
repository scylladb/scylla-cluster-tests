# NoSQLBench Docker Image for ScyllaDB Java Driver v4

## Overview

This directory contains a Docker setup to run NoSQLBench with the ScyllaDB Java driver v4. The setup uses a custom shell script to replace the Apache Cassandra driver with the ScyllaDB driver, allowing comprehensive testing and benchmarking of ScyllaDB Java driver.

## Contents

* **change-to-scylladb-driver.sh**: A script to replace the Apache Cassandra driver with the ScyllaDB driver. It accepts one argument specifying the ScyllaDB driver version.
* **build.sh**: A script to build the Docker image with configurable options.
* **Dockerfile**: Custom `nosqlbench` Dockerfile
* **nosqlbench**: shell for running `nosqlbench` in docker as `CMD`

## Building `nosqlbench` locally

* Bash
* Git
* xmlstarlet
* maven
* JDK 21+
* rsync

### Commands to Run

1. Clone the NoSQLBench repository:

```sh
git clone --depth 1 https://github.com/nosqlbench/nosqlbench.git
cd nosqlbench
```

2. Fetch the tags:

```sh
git fetch --tags
```

3. Check out the desired release version:

```sh
git checkout -b 5.21.2-release tags/5.21.2-release
```

4. Replace the Apache Cassandra driver with the ScyllaDB driver:

```sh
./change-to-scylladb-driver.sh 4.18.0.1
```

5. Build the project using Maven

```sh

mvn \
    -P!build-nb5-appimage \
    -P!build-nbr-appimage \
   -DskipTests=true \
   package
```

6. Copy the built jar file to the bin directory:

```sh
cp nb5/target/nb5.jar bin/nb.jar
```

7. Running the Jar File

To run the NoSQLBench jar file, use the following command:

```sh
java -jar bin/nb5.jar <arguments>
```

Replace \<arguments> with any NoSQLBench command-line arguments.

## How It Works

The Docker image is configured to use NoSQLBench and the ScyllaDB Java driver v4. The change-to-scylladb-driver.sh script is used during the image build process to replace the Apache Cassandra driver with the ScyllaDB driver. This setup ensures that all NoSQLBench operations are performed using the ScyllaDB driver, providing accurate benchmarking results.
Building the Docker Image

To build the Docker image, use the build.sh script. The script accepts the following arguments:

* **-v**: NoSQLBench version from GitHub (required)
* **-d**: ScyllaDB driver version (required)
* **-g**: GitHub repository URL of NoSQLBench (optional, default is the official repository)
* **-t**: Tag name for the Docker image (optional, default is scylladb/hydra-loaders:nosqlbench-\<NoSQLBench version>)

## Dockerbuild

Before you begin, ensure you have the following installed on your local machine:

* Docker
* Bash

### Example

```sh
./build.sh -v "5.12.2-release" -d "4.18.0.1" -t "scylladb/hydra-loaders:nosqlbench-5.12.2"
```

Running the above command will generate a Docker image tagged scylladb/hydra-loaders:nosqlbench-5.12.2, using NoSQLBench version 5.21.2 and ScyllaDB Java driver version 4.18.0.1

## Running NoSQLBench from the Docker Image

To run NoSQLBench using the built Docker image, use the following command:

```sh
docker run -it scylladb/hydra-loaders:nosqlbench-<NoSQLBench version> nosqlbench <additional-arguments>
```

Replace \<NoSQLBench version> with the version specified during the image build and \<additional-arguments> with any NoSQLBench command-line arguments.

## Conclusion

This setup provides a streamlined way to test and benchmark ScyllaDB Java driver using NoSQLBench.
