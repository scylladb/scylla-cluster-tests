# FAQ

## Reusing already running Cluster

Running a test with already provisioned cluster, you can get the test_id in the AWS console or from jenkins logs:

```
# add the following to your config yaml
reuse_cluster: 7c86f6de-f87d-45a8-9e7f-61fe7b7dbe84

# or with using the new configuration, before the run test command
export SCT_REUSE_CLUSTER=7c86f6de-f87d-45a8-9e7f-61fe7b7dbe84
```

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
