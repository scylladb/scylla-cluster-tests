# FAQ

## Reusing already running Cluster

See: [reuse_cluster](./reuse_cluster.md)

## using SCT_UPDATE_DB_PACKAGES to override AMI with newer binaries

SCT has the ability to run an upgrade to a given RPM, that will happen either after a regular installation or a deployment of an instance. The desired RPM must be placed somewhere in the builder, that will copy it to the DB node and run a rpm command to upgrade the installed package (be sure that your RPM has a version bigger than the one installed).::

```bash
# from your environment variables set like this:
# be sure to put a slash after the path !
export SCT_UPDATE_DB_PACKAGES=<path_to_my_rpm>/

# from your jenkinsfile file you could set like this (inside your pipeline settings):
update_db_packages: '<path_to_my_rpm>/'

# from your yaml file set like this:
update_db_packages: '<path_to_my_rpm>/'
```

## use SCT_UPDATE_DB_PACKAGES in Jenkins straight from s3/gs

SCT now support passing s3:// or gs:// urls in update_db_packages, for example:

```bash
# uploading to s3
aws s3 cp s3://downloads.scylladb.com/
aws s3 cp --recursive rpms s3://scylla-qa-public/`whoami`/

# download from s3 path
export SCT_UPDATE_DB_PACKAGES=s3://scylla-qa-public/`whoami`/rpms

# uploading to google storage
gsutil cp rpms/* gs://scratch.scylladb.com/`whoami`/rpms/

# download for google storage
export SCT_UPDATE_DB_PACKAGES=gs://scratch.scylladb.com/`whoami`/rpms

# downloading a specific rpms built on master in job 888
export SCT_UPDATE_DB_PACKAGES=s3://downloads.scylladb.com/rpm/unstable/centos/master/888/scylla/7/x86_64/
```

## How to use SCT_SCYLLA_MGMT_PKG

SCT has the ability to run a job (manager jobs) using your own scylla-manager package files.
It will allow you to run one of the manager jobs using your self build package files, for example:

```bash
# uploading to s3
aws s3 cp --recursive rpms s3://scylla-qa-public/`whoami`/

# download from s3 path
export SCT_SCYLLA_MGMT_PKG=s3://scylla-qa-public/`whoami`/rpms

# uploading to google storage
gsutil cp rpms/* gs://scratch.scylladb.com/`whoami`/rpms/

# download for google storage
export SCT_SCYLLA_MGMT_PKG=gs://scratch.scylladb.com/`whoami`/rpms

# downloading specific rpms built on master in job 762
export SCT_SCYLLA_MGMT_PKG=s3://downloads.scylladb.com/manager/rpm/unstable/centos/master/762/scylla-manager/7/x86_64/

# using a local path, place your rpms into a local folder inside the builder
export SCT_SCYLLA_MGMT_PKG=<path_to_my_rpms>/
```

## exposing nemesis metrics from local dev machine

since your computer isn't exposed to the internet, the monitor can't reach it

```bash
# ngrok can be used to help with it
# goto https://ngrok.com/download, then in a separate terminal window
./ngrok start --none

# back when you want to run your test
export SCT_NGROK_NAME=`whoami`

# run you test
hydra run-test ....

# while test running your metrics api would be exposed for example:
# http://fruch.ngrok.io
```

## How can I connect to test machines in AWS ?

```bash
# option 1 - with hydra ssh wrapper command

# list all the machine this user create, and let you choose one to connect:
hydra ssh --user `whoami`

# connect with name of the machine
hydra ssh perf-test-db-node-3

# connect to specific test machines by test_id
hydra ssh --test-id 123456

# option 2 - open the ports publicly, and connect with key
hydra attach-test-sg --user `whoami`

? Select machine:  (Use arrow keys to move, <space> to select, <a> to toggle, <i> to invert)
 » ● sct-runner-1.6-instance-3dc7fd08 - 52.211.133.49 10.4.3.84 - eu-west-1
   ● sct-runner-1.6-instance-e60d9d6b - 3.252.145.58 10.4.1.201 - eu-west-1
   ● sct-runner-1.6-instance-6f4e6365 - 3.252.203.249 10.4.3.133 - eu-west-1
   ● None - 44.200.18.106 10.12.2.222 - us-east-1
   ● sct-runner-1.6-instance-d1df7c5f - 3.232.134.209 10.12.2.167 - us-east-1
   ● sct-runner-1.6-instance-e596851f - 44.192.58.53 10.12.3.16 - us-east-1
   ● sct-runner-1.6-instance-607c5973 - 3.236.43.235 10.12.2.11 - us-east-1
   ● sct-runner-1.6-instance-988e89c7 - 44.192.128.164 10.12.1.184 - us-east-1

# select which machine you want to expose publicly, and a SG would be attached to them
ssh -i ~/.ssh/scylla_test_id_ed25519 ubuntu@44.192.58.53
# keep in mind the user name and key, can be different between backend or between tests
```

## How can I clear monitoring stack create by `hydra investigate show-monitor`

```bash
# this would clear all of the dockers used by monitoring stack that are currently running
docker rm -f -v $(docker ps --filter name=agraf\|aprom\|aalert -a -q)
```

## How can I upload a relevant log file that was missed by SCT and report it to Argus?

The command `upload` provides such functionality:

```bash
hydra upload --test-id <uuid> path/to/file
```

In case you don't want to report to Argus / Argus is missing the test run for this id you can use `--no-use-argus` to skip that part.


## How to open a coredump from sct test run (python) ?

first download the file to the SCT folder, and execute the following command:
```bash
./docker/env/hydra.sh 'bash -c "sudo pip install pystack; pystack core core.python3.1000.bd43fbcd0c4b44488ce7e97e25fe1a28.1804.1745768005000000"'
```


## How to define parameters for performance throughput test ?
There are 3 parameters that can be defined in the yaml file to control the performance throughput test (example here: `configurations/performance/cassandra_stress_gradual_load_steps.yaml`):

```
perf_gradual_throttle_steps - Define throttle for load steps in ops per sub-test (read / write / mixed).
                                Example: {'read': ['100000', '150000'], 'write': [200, unthrottled], 'mixed': ['300']}
```
```
perf_gradual_threads - Threads amount of stress load per sub-test (read / write / mixed).
                       For debugging, you can set a specific thread count for each step (per load).
                       The value of perf_gradual_threads[load] must be either:
                           - a single-element list or integer(applied to all throttle steps)
                                Example: {'read': 100, 'write': 200, 'mixed': [200, 300]}

                           - a list with the same length as perf_gradual_throttle_steps[load] (one thread count per step).
                                Example: {'read': [100, 200], 'write': [200, 300], 'mixed': [300]}
```
```
perf_gradual_throttle_duration - Define duration for each step in seconds per sub-test (read / write / mixed).
                                    Example: {'read': '30m', 'write': None, 'mixed': '30m'}
```

Those parameters can be overridden during job triggering by setting the environment variables in `extra_environment_variables`:
```bash
SCT_PERF_GRADUAL_THREADS={"read": [450, 400, 450], "write": 400, "mixed": 1900}
SCT_PERF_GRADUAL_THROTTLE_STEPS={"read": ['700000', 'unthrottled', 'unthrottled'], "mixed": ['50000', '150000', '300000', '450000', 'unthrottled'], "write": ['200000', '300000', 'unthrottled']}
```

## How to find equivalent AMIs across regions or architectures?

Use the `find-ami-equivalent` command to find equivalent AWS AMIs in different regions or architectures based on image tags:

```bash
# Find equivalent in different regions
./sct.py find-ami-equivalent \
    --ami-id ami-0d9726c9053daff76 \
    --source-region us-east-1 \
    --target-region us-west-2 \
    --target-region eu-west-1

# Find ARM64 equivalent of an x86_64 AMI
./sct.py find-ami-equivalent \
    --ami-id ami-0d9726c9053daff76 \
    --source-region us-east-1 \
    --target-arch arm64

# Get JSON output for pipeline automation
./sct.py find-ami-equivalent \
    --ami-id ami-0d9726c9053daff76 \
    --source-region us-east-1 \
    --target-region us-west-2 \
    --output-format json
```

The command matches AMIs based on ScyllaDB's tagging conventions (Name, scylla_version, build-id) and returns results sorted by creation date.
