SCT - Scylla Cluster Tests
##########################

SCT tests are designed to test Scylla database on physical/virtual servers under high read/write load.
Currently the tests are run using built in unittest
These tests automatically create:

* Scylla clusters - Run Scylla database
* Loader machines - used to run load generators like cassandra-stress
* Monitoring server - uses official Scylla Monitoring repo_ to monitor Scylla clusters and Loaders

What's inside?
==============

1. A library, called ``sdcm`` (stands for scylla distributed cluster
   manager). The word 'distributed' was used here to differentiate
   between that and CCM, since with CCM the cluster nodes are usually
   local processes running on the local machine instead of using actual
   machines running scylla services as cluster nodes. It contains:

   * ``sdcm.cluster``: Base classes for Clusters
   * ``sdcm.remote``: SSH library
   * ``sdcm.nemesis``: Nemesis classes (a nemesis is a class that does disruption in the node)
   * ``sdcm.tester``: Contains the base test class, see below.

2. A data directory, aptly named ``data_dir``. It contains:

   * scylla repo file (to prepare a loader node)
   * Files that need to be copied to cluster nodes as part of setup/test procedures
   * yaml file containing test config data:

     * AWS machine image ids
     * Security groups
     * Number of loader nodes
     * Number of cluster nodes
   * SCT dashboards definition files for Grafana

3. Python files with tests. The convention is that test files have the ``_test.py`` suffix.
4. Utilities directory named ``utils``. Contains help utilities for log analizing

Maintained branches
===================

https://github.com/scylladb/scylla-cluster-tests/wiki

Contribution
============
Since we are trying to keep the code neat, please install this git precommit hooks, that would fix the code style, and run more checks::

    pip install pre-commit==1.14.4
    pre-commit install
    pre-commit install --hook-type commit-msg

If you want to remove the hook::

    pre-commit uninstall

Doing a commit without the hook checks::

    git commit ... -n


Setting up SCT environment
==========================

Currently we support Red Hat like operating systems that use YUM package manager.
SCT tests can run on two environments: on local RHEL like OS (tested on Fedora) or inside SCT docker container.
Note: When running following commands, please clone this repo and `cd` into it.
During SCT environment setup, you will be asked to configure your AWS CLI tool. This is needed to retrieve
QA private keys from secure S3 Bucket during automated setup.
The keys will be needed to connect to the Scylla clusters under test via SSH.

Ask your AWS account admin to create a user and access key for AWS) and then configure AWS::

    > aws configure
    AWS Access Key ID [****************7S5A]:
    AWS Secret Access Key [****************5NcH]:
    Default region name [us-east-1]:
    Default output format [None]:


From here you can proceed with one of the 2 options

Option 1: Setup SCT in Docker
-----------------------------
As mentioned before, instead of installing all the prerequisites on your machine, you can also use SCT Docker
container (aka Hydra) to run SCT tests::

    sudo ./install-hydra.sh

Notes for Hydra

 * When running Hydra for the first time it will build the SCT Docker image. Please be patient and let the process complete till the end
 * Your home directory is exposed into the docker container to the root user, so all the SSH/AWS/GCE configurations are "automatically" visible to the SCT container.
 * SCT is the current working directory in the container ( Run `hydra ls -l` to check)
 * QA private keys existence is checked each time when Hydra is run.
 * Hydra will check for update on each run and when update will be available the Docker image will be rebuilt

Option 2: Setup SCT locally
---------------------------

To run SCT tests locally run following::

    sudo ./install-prereqs.sh
    ./get-qa-ssh-keys.sh

    # install python3.10 via pyenv
    curl https://pyenv.run | bash
    exec $SHELL
    # go to: https://github.com/pyenv/pyenv/wiki/Common-build-problems#prerequisites
    # and follow the instructions for your distribution, to install the prerequisites
    # for compiling python from source
    pyenv install 3.10.0

    # create a virtualenv for SCT
    pyenv virtualenv 3.10.0 sct310
    pyenv activate sct310
    pip install -r requirements.in



Run a test
----------

Example running test using Hydra using `test-cases/PR-provision-test.yaml` configuration file

on AWS::

    hydra "run-test longevity_test.LongevityTest.test_custom_time --backend aws --config test-cases/PR-provision-test.yaml"

on AWS using SCT Runner::

    hydra create-runner-instance --cloud-provider <cloud_name> -r <region_name> -z <az> -t <test-id> -d <run_duration>
    hydra --execute-on-runner <runner-ip|`cat sct_runner_ip> "run-test longevity_test.LongevityTest.test_custom_time --backend aws --config test-cases/PR-provision-test.yaml"

on GCE::

    hydra "run-test longevity_test.LongevityTest.test_custom_time --backend gce --config test-cases/PR-provision-test.yaml"


You can also enter the containerized SCT environment using::

    hydra bash

Depending on which backend hardware/cloud provider/virtualization you will use, relevant configuration of those backend
services should be done.

List resources being used::

    # google cloud engine
    gcloud compute instances list --filter="metadata.items.key['RunByUser']['value']='`whoami`'"

    # amazon
    aws ec2 describe-instances --query 'Reservations[].Instances[].InstanceId' --filter "Name=tag:RunByUser,Values=`whoami`"

    # both GCE and AWS
    hydra list-resources --user `whoami`

Configuring test run configuration YAML
---------------------------------------

Take a look at the ``test-cases/PR-provision-test.yaml`` file. It contains a number of
configurable test parameters, such as DB cluster instance types and AMI IDs.
In this example, we're assuming that you have copied ``test-cases/PR-provision-test.yaml``
to ``test-cases/your_config.yaml``.

All the test run configurations are stored in ``test-cases`` directory.

Important: Some tests use custom hardcoded operations due to their nature,
so those tests won't honor what is set in ``test-cases/your_config.yaml``.

Run the tests
=============

AWS - Amazon Web Services
-------------------------

Change your current working directory to this test suite base directory,
then run test. Example command line::

    hydra run-test longevity_test.LongevityTest.test_custom_time --backend aws --config test-cases/your_config.yaml

This command line is to run the test method ``test_custom_time``, in
the class ``Longevitytest``, that lies inside the file ``longevity_test.py``,
and the test will run using the AWS data defined in the branch ``eu_west_1``
of ``data_dir/your_config.yaml``.

Reuse Cluster (AWS)
^^^^^^^^^^^^^^^^^^^

Running a test with already provisioned cluster, you can get the test_id in the AWS console of the one of the nodes tags tab::

    # add the following to your config yaml
    reuse_cluster: 7c86f6de-f87d-45a8-9e7f-61fe7b7dbe84

    # or with using the new configuration, before the run test command
    export SCT_REUSE_CLUSTER=7c86f6de-f87d-45a8-9e7f-61fe7b7dbe84



GCE - Google Compute Engine
---------------------------

In order to run tests using the GCE backend, you'll need:

1. A GCE account

2. `cp data_dir/scylla.yaml data_dir/your_config.yaml`

3. Edit the configuration file (data_dir/your_config.yaml) to tweak values present
   in the `gce:` session of that file. One of the values you might want to
   tweak is the scylla yum repository used to install scylla on the CentOS 7 image.

With that said and done, you can run your test using the command line::

    hydra run-test longevity_test.LongevityTest.test_custom_time --backend gce --config test-cases/scylla-lmr.yaml


Docker
------
**NOTE:** for docker run work with monitoring stack, user should be part of sudo group,
and setup with passwordless access, see https://unix.stackexchange.com/a/468417 for example on how to setup

We can also enable running with scylla formal docker images::

    # example of running specific docker version
    export SCT_SCYLLA_VERSION=3.2.rc1
    hydra run-test longevity_test.LongevityTest.test_custom_time --backend docker --config test-cases/PR-provision-test-docker.yaml

(Optional) Follow what the test is doing
========================================

What you can do while the test is running to see what's happening::

    tail -f ~/sct-results/latest/sct.log

Test operations
===============

On a high level overview, the test operations are:

Setup
-----

1) Instantiate a Cluster DB, with the specified number of nodes (the number
   of nodes can be specified through the config file, or the test writer can
   set a specific number depending on the test needs).

2) Instantiate a set of loader nodes. They will be the ones to initiate
   cassandra stress, and possibly other database stress inducing activities.

3) Instantiate a set of monitoring nodes. They will run prometheus [3], to
   store metrics information about the database cluster, and also grafana [4],
   to let the user see real time dashboards of said metrics while the test is
   running. This is very useful in case you want to run the test suite and keep
   watching the behavior of each node.

4) Wait until the loaders are ready (SSH up and cassandra-stress is present)

5) Wait until the DB nodes are ready (SSH up and DB services are up, port 9042
   occupied)

6) Wait until the monitoring nodes are ready. If you are following the job log,
   you will see a message with the address you can point your browser to while
   the test is executing ::

    02:09:37 INFO | Node lmr-scylla-monitor-node-235cdfb0-1 [54.86.66.156 | 172.30.0.105] (seed: None): Grafana Web UI: http://54.86.66.156:3000

Actual test
-----------

1) Loader nodes execute cassandra stress on the DB cluster (optional)

2) If configured, a Nemesis class, will execute periodically, introducing some
   disruption activity to the cluster (stop/start a node, destroy data, kill
   scylla processes on a node). the nemesis starts after an interval, to give
   cassandra-stress on step 1 to stabilize

Keep in mind that the suite libraries are flexible, and will allow you to
set scenarios that differ from this base one.

Making sense of logs
====================

In order to try to establish a timeline of what is going on, we opted for
dumping a lot of information in the test main log. That includes:

1) Labels for each Node and cluster, including SSH access info in case
   you want to debug what's going on. Example::

    15:43:23 DEBUG| Node lmr-scylla-db-node-88c994d5-1 [54.183.240.195 | 172.31.18.109] (seed: None): SSH access -> 'ssh -i /var/tmp/lmr-longevity-test-8b95682d.pem centos@54.183.240.195'
    ...
    15:47:52 INFO | Cluster lmr-scylla-db-cluster-88c994d5 (AMI: ami-1da7d17d Type: c4.xlarge): (6/6) DB nodes ready. Time elapsed: 79 s

2) Scylla logs for all the DB nodes, logged as they happen. Example line::

    15:44:35 DEBUG| [54.183.193.208] [stdout] Feb 10 17:44:17 ip-172-30-0-123.ec2.internal systemd[1]: Starting Scylla Server...

3) Coredump watching thread, that runs every 30 seconds and will tell you if
   scylla dumped core

4) Cassandra-stress output. As cassandra-stress runs only after all the nodes
   are properly set up, you'll see it clearly separated from the initial flurry
   of Node init information::

    15:47:55 INFO | [54.193.84.90] Running '/usr/bin/ssh -a -x  -o ControlPath=/var/tmp/ssh-masterTQ3hZu/socket -o StrictHostKeyChecking=no -o UserKnownHostsFile=/var/tmp/tmpOjFA9Q -o BatchMode=yes -o ConnectTimeout=300 -o ServerAliveInterval=300 -l centos -p 22 -i /var/tmp/lmr-longevity-test-8b95682d.pem 54.193.84.90 "cassandra-stress write cl=QUORUM duration=30m -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=3)' -mode cql3 native -rate threads=4 -node 172.31.18.109"'
    15:48:02 DEBUG| [54.193.84.90] [stdout] INFO  17:48:01 Found Netty's native epoll transport in the classpath, using it
    15:48:03 DEBUG| [54.193.84.90] [stdout] INFO  17:48:03 Using data-center name 'datacenter1' for DCAwareRoundRobinPolicy (if this is incorrect, please provide the correct datacenter name with DCAwareRoundRobinPolicy constructor)
    15:48:03 DEBUG| [54.193.84.90] [stdout] INFO  17:48:03 New Cassandra host /172.31.18.109:9042 added
    15:48:03 DEBUG| [54.193.84.90] [stdout] INFO  17:48:03 New Cassandra host /172.31.18.114:9042 added
    15:48:03 DEBUG| [54.193.84.90] [stdout] INFO  17:48:03 New Cassandra host /172.31.18.113:9042 added
    15:48:03 DEBUG| [54.193.84.90] [stdout] INFO  17:48:03 New Cassandra host /172.31.18.112:9042 added
    15:48:03 DEBUG| [54.193.84.90] [stdout] INFO  17:48:03 New Cassandra host /172.31.18.111:9042 added
    15:48:03 DEBUG| [54.193.84.90] [stdout] INFO  17:48:03 New Cassandra host /172.31.18.110:9042 added
    15:48:03 DEBUG| [54.193.84.90] [stdout] Connected to cluster: lmr-scylla-db-cluster-88c994d5
    ...

5) As the DB logs thread will still be active, you'll see messages from nodes
   (normally compaction) mingled with cassandra-stress output. Example::

    16:01:43 DEBUG| [54.193.84.90] [stdout] total,       2265875,    4887,    4887,    4887,     0.8,     0.6,     2.5,     3.6,     9.8,    13.8,  493.7,  0.00632,      0,      0,       0,       0,       0,       0
    16:01:44 DEBUG| [54.193.84.90] [stdout] total,       2270561,    4679,    4679,    4679,     0.8,     0.6,     2.5,     3.6,     8.1,    10.1,  494.7,  0.00630,      0,      0,       0,       0,       0,       0
    16:01:45 DEBUG| [54.183.240.195] [stdout] Feb 10 18:01:45 ip-172-31-18-109 scylla[2103]: INFO  [shard 1] compaction - Compacting [/var/lib/scylla/data/keyspace1/standard1-71035bf0d01e11e58c82000000000001/keyspace1-standard1-ka-5-Data.db:level=0, /var/lib/scylla/data/keyspace1/standard1-71035bf0d01e11e58c82000000000001/keyspace1-standard1-ka-9-Data.db:level=0, /var/lib/scylla/data/keyspace1/standard1-71035bf0d01e11e58c82000000000001/keyspace1-standard1-ka-13-Data.db:level=0, /var/lib/scylla/data/keyspace1/standard1-71035bf0d01e11e58c82000000000001/keyspace1-standard1-ka-17-Data.db:level=0, ]
    16:01:45 DEBUG| [54.193.84.90] [stdout] total,       2275544,    4963,    4963,    4963,     0.8,     0.6,     2.4,     3.4,     9.7,    18.9,  495.7,  0.00629,      0,      0,       0,       0,       0,       0
    16:01:46 DEBUG| [54.193.84.90] [stdout] total,       2280432,    4883,    4883,    4883,     0.8,     0.6,     2.5,     3.6,    15.4,    20.2,  496.7,  0.00628,      0,      0,       0,       0,       0,       0
    16:01:47 DEBUG| [54.193.84.90] [stdout] total,       2285011,    4562,    4562,    4562,     0.9,     0.6,     2.5,     3.8,    18.2,    30.9,  497.7,  0.00627,      0,      0,       0,       0,       0,       0


6) You'll also see Nemesis messages. The cool thing about this is that you can see
   the cluster reaction to the disruption event. Here's an example of a nemesis
   that stops and then starts the AWS instance of one of our DB nodes. Ellipsis
   were added for brevity purposes. You can see the gossiping for the node down,
   then for the Node up, all of that happening while the loader nodes churning
   cassandra-stress output::

    15:57:55 DEBUG| sdcm.nemesis.StopStartMonkey: <function disrupt at 0x7fd5aec38c80> Start
    15:57:55 INFO | sdcm.nemesis.StopStartMonkey: Stop Node lmr-scylla-db-node-88c994d5-3 [54.193.37.181 | 172.31.18.111] (seed: False) then restart it
    15:57:55 DEBUG| [54.193.84.90] [stdout] total,       1257018,    4989,    4989,    4989,     0.8,     0.6,     2.4,     2.9,     9.9,    23.1,  265.3,  0.00651,      0,      0,       0,       0,       0,       0
    15:57:56 DEBUG| [54.193.84.90] [stdout] total,       1262289,    5248,    5248,    5248,     0.7,     0.6,     2.4,     2.8,     5.9,     7.0,  266.4,  0.00650,      0,      0,       0,       0,       0,       0
    15:57:57 DEBUG| [54.193.37.181] [stdout] Feb 10 17:57:56 ip-172-31-18-111 systemd[1]: Stopping Scylla JMX...
    15:57:57 DEBUG| [54.183.195.134] [stdout] Feb 10 17:57:57 ip-172-31-18-112 scylla[2108]: INFO  [shard 0] gossip - InetAddress 172.31.18.111 is now DOWN
    15:57:57 DEBUG| [54.183.193.208] [stdout] Feb 10 17:57:57 ip-172-31-18-113 scylla[2114]: INFO  [shard 0] gossip - InetAddress 172.31.18.111 is now DOWN
    15:57:57 DEBUG| [54.193.37.222] [stdout] Feb 10 17:57:57 ip-172-31-18-114 scylla[2098]: INFO  [shard 0] gossip - InetAddress 172.31.18.111 is now DOWN
    15:57:57 DEBUG| [54.193.61.5] [stdout] Feb 10 17:57:57 ip-172-31-18-110 scylla[2107]: INFO  [shard 0] gossip - InetAddress 172.31.18.111 is now DOWN
    15:57:57 DEBUG| [54.183.240.195] [stdout] Feb 10 17:57:57 ip-172-31-18-109 scylla[2103]: INFO  [shard 0] gossip - InetAddress 172.31.18.111 is now DOWN
    15:57:57 DEBUG| [54.193.84.90] [stdout] total,       1267035,    4739,    4739,    4739,     0.8,     0.6,     2.4,     4.8,    17.7,    30.2,  267.4,  0.00647,      0,      0,       0,       0,       0,       0
    ...
    15:58:01 DEBUG| [54.193.84.90] [stdout] total,       1283680,    4219,    4219,    4219,     0.9,     0.6,     2.6,     4.4,     8.1,    11.9,  271.4,  0.00651,      0,      0,       0,       0,       0,       0
    15:58:02 DEBUG| [54.193.84.90] [stdout] total,       1285139,    1452,    1452,    1452,     2.7,     1.7,     9.2,    22.3,    54.8,    55.2,  272.4,  0.00699,      0,      0,       0,       0,       0,       0
    15:58:02 DEBUG| [54.183.240.195] [stdout] Feb 10 17:58:02 ip-172-31-18-109 scylla[2103]: INFO  [shard 0] rpc - client 172.31.18.111: client connection dropped: read: Connection reset by peer
    15:58:02 DEBUG| [54.193.37.222] [stdout] Feb 10 17:58:02 ip-172-31-18-114 scylla[2098]: INFO  [shard 0] rpc - client 172.31.18.111: client connection dropped: read: Connection reset by peer
    15:58:02 DEBUG| [54.193.61.5] [stdout] Feb 10 17:58:02 ip-172-31-18-110 scylla[2107]: INFO  [shard 0] rpc - client 172.31.18.111: client connection dropped: read: Connection reset by peer
    15:58:02 DEBUG| [54.183.193.208] [stdout] Feb 10 17:58:02 ip-172-31-18-113 scylla[2114]: INFO  [shard 0] rpc - client 172.31.18.111: client connection dropped: read: Connection reset by peer
    15:58:03 DEBUG| [54.193.84.90] [stdout] total,       1288782,    3515,    3515,    3515,     1.1,     0.6,     2.6,     7.7,    56.3,   143.6,  273.4,  0.00701,      0,      0,       0,       0,       0,       0
    ...
    15:58:59 DEBUG| [54.193.84.90] [stdout] total,       1532519,    4846,    4846,    4846,     0.8,     0.6,     2.5,     3.8,     9.5,    10.9,  328.8,  0.00715,      0,      0,       0,       0,       0,       0
    15:58:59 DEBUG| Node lmr-scylla-db-node-88c994d5-3 [54.193.37.181 | 172.31.18.111] (seed: None): Got new public IP 54.67.92.86
    15:59:00 DEBUG| [54.193.84.90] [stdout] total,       1537219,    4681,    4681,    4681,     0.8,     0.6,     2.5,     3.9,    18.8,    28.3,  329.8,  0.00713,      0,      0,       0,       0,       0,       0
    ...
    15:59:51 DEBUG| [54.193.37.222] [stdout] Feb 10 17:59:51 ip-172-31-18-114 scylla[2098]: INFO  [shard 0] gossip - Node 172.31.18.111 has restarted, now UP
    15:59:52 DEBUG| [54.193.84.90] [stdout] total,       1767965,    4869,    4869,    4869,     0.8,     0.6,     2.5,     3.0,    12.3,    15.0,  382.1,  0.00677,      0,      0,       0,       0,       0,       0
    15:59:52 DEBUG| [54.183.240.195] [stdout] Feb 10 17:59:52 ip-172-31-18-109 scylla[2103]: INFO  [shard 0] gossip - Node 172.31.18.111 has restarted, now UP
    15:59:53 DEBUG| [54.193.84.90] [stdout] total,       1771279,    3291,    3291,    3291,     1.2,     0.6,     3.4,    13.2,    32.3,    39.8,  383.1,  0.00680,      0,      0,       0,       0,       0,       0
    15:59:53 DEBUG| [54.193.61.5] [stdout] Feb 10 17:59:53 ip-172-31-18-110 scylla[2107]: INFO  [shard 0] gossip - Node 172.31.18.111 has restarted, now UP
    15:59:54 DEBUG| [54.193.84.90] [stdout] total,       1775909,    4622,    4622,    4622,     0.9,     0.6,     2.5,     3.7,     9.9,    16.3,  384.1,  0.00678,      0,      0,       0,       0,       0,       0
    15:59:54 DEBUG| [54.183.195.134] [stdout] Feb 10 17:59:54 ip-172-31-18-112 scylla[2108]: INFO  [shard 0] gossip - Node 172.31.18.111 has restarted, now UP

With all that information going, the main log is hard to read, but at least
you now have an outline of what is going on. We store the scylla logs
on per node files, you can find them all in the test log directory

SCT utilities
=============

1) utils/fetch_and_decode_stalls_from_job_database_logs.sh

This script searches in the log all reactor stalles, find unique stalles and decode them.
The script analyzes the database.logs that are located under ~/sct-results/<job-folder>/<test-folder>/<cluster-folder>. The script is going through nodes folders and analyze database.log for every node.

2) utils/fetch_and_decode_stalls_from_journalctl_logs_all_nodes.sh -

This script searches in the journalctl all reactor stalles, find unique stalles and decode them.
Save the journalctl from every node to the database.log and move to the folders by node. Organize all folders in one folder, like::

    logs/node1/database.log
    logs/node2/database.log

3) utils/fetch_and_decode_stalls_from_one_journalctl_log.sh

This script searches in the one journalctl all reactor stalles, find unique stalles and decode them.
Save the journalctl of one node to the database.log and move to the folder

For examples see utilities documentation

Building Hydra Docker image
===========================

Once you have changes in the requirements.in or in Hydra Dockerfile

- change the version in docker/env/version
- run ``./docker/env/build_n_push.sh`` to build and push to Docker Hub

SCT test profiling
==================

- set environment variable "SCT_ENABLE_TEST_PROFILING" to 1, or add "enable_test_profiling: true" into yaml file
- run test

After test is done there are following ways to use collected stats:
- cat ~/latest/profile.stats/stats.txt
- snakeviz ~/latest/profile.stats/stats.bin
- tuna ~/latest/profile.stats/stats.bin
- gprof2dot -f pstats ~/latest/profile.stats/stats.bin | dot -Tpng -o ~/latest/profile.stats/stats.png

Another way to profile is py-spy:
- pip install py-spy
Run recording:
- py-spy record -s -o ./py-spy.svg -- python3 sct.py ...
Run 'top' mode:
- py-spy top -s -- python3 sct.py ...

Creating pipeline jobs for new branch
=====================================

Once a new branch is create, we could build all the need job for this branch with the following script ::

    JENKINS_USERNAME=[jenkins username] JENKINS_PASSWORD=[token from jenkins] hydra create-test-release-jobs scylla-4.0 --sct_branch branch-4.0
    # enterprise features
    JENKINS_USERNAME=[jenkins username] JENKINS_PASSWORD=[token from jenkins] hydra create-test-release-jobs-enterprise enterprise-2020.1 --sct_branch branch-2020.1

Creating pipeline jobs for new scylla-operator branch/release/tag
=================================================================

Create new set of scylla-operator jobs using following command ::

    hydra create-operator-test-release-jobs \
      operator-1.2 \
      jenkins-username \
      jenkins-user-password-or-api-token \
      --sct_branch dev \
      --sct_repo git@github.com:some-github-username-123321/scylla-cluster-tests.git

FAQ
====
**Q:** My c-s and memesis metrics are not exposed to the monitor while running locally, why ?

**A:** since your computer isn't exposed to the internet, the monitor can't reach it::

    # ngrok can be used to help with it
    # goto https://ngrok.com/download, then in a separate terminal window
    ./ngrok start --none

    # back when you want to run your test
    export SCT_NGROK_NAME=`whoami`

    # run you test
    hydra run-test ....

    # while test running your metrics api would be exposed for example:
    # http://fruch.ngrok.io


**Q:** How to use SCT_UPDATE_DB_PACKAGES on my job, and what does it do ?

**A:** SCT has the ability to run an upgrade to a given RPM, that will happen either after a regular installation or a deployment of an instance. The desired RPM must be placed somewhere in the builder, that will copy it to the DB node and run a rpm command to upgrade the installed package (be sure that your RPM has a version bigger than the one installed).::

    # from your environment variables set like this:
    # be sure to put a slash after the path !
    export SCT_UPDATE_DB_PACKAGES=<path_to_my_rpm>/

    # from your jenkinsfile file you could set like this (inside your pipeline settings):
    update_db_packages: '<path_to_my_rpm>/'

    # from your yaml file set like this:
    update_db_packages: '<path_to_my_rpm>/'

**Q:** I want to use SCT_UPDATE_DB_PACKAGES but Jenkins keep selecting different builder, what can I do

**A:** SCT now support passing s3:// or gs:// urls in update_db_packages, for example ::

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

**Q:** How to use SCT_SCYLLA_MGMT_PKG and what does it do?

**A:** SCT has the ability to run a job (manager jobs) using your own scylla-manager package files. It will allow you to run one of the manager jobs using your self build package files, for example ::

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

**Q:** How do I update the configuration docs ?

**A:** Like that ::

    hydra update-conf-docs


Run a functional test
=====================

Functional tests are stored in ``functional_tests/`` directory
You can use any configuration file for them, but in general you need ``test-cases/scylla-operator/functional.yaml``
You can use any backend, but for scylla_operator tests it needs to be any kubernetes backend,
such as ``k8s-eks, k8s-gke, k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce``

Hardware requirements:
    You need to have at least **16Gb** of RAM and **120Gb** of disk to get it running

After the run tests logs are stored in the directory you passed to --logdir, or to ``~/sct-results`` if you did not

Running in hydra
----------------

on EKS::

    hydra "run-pytest functional_tests/scylla_operator --backend k8s-eks --config test-cases/scylla-operator/functional.yaml --logdir='`pwd`'"

on Local kind cluster::

    hydra "run-pytest functional_tests/scylla_operator --backend k8s-local-kind --config test-cases/scylla-operator/functional.yaml  --logdir='`pwd`'"

Running via sct.py
------------------

The benefit of running in this mode, is that you can reuse your local kind binary

on EKS::

    sct.py run-pytest functional_tests/scylla_operator --backend k8s-eks --config test-cases/scylla-operator/functional.yaml --logdir="`pwd`"

on Local kind cluster::

    sct.py run-pytest functional_tests/scylla_operator --backend k8s-local-kind --config test-cases/scylla-operator/functional.yaml --logdir="`pwd`"

Running via python
------------------

The benefit of running in this mode, is that not only you can reuse your local kind binary
But you also can use breakpoints to debug tests

on EKS::

    SCT_CLUSTER_BACKEND=k8s-eks SCT_CONFIG_FILES=test-cases/scylla-operator/functional.yaml python -m pytest functional_tests/scylla_operator

on Local kind cluster::

    SCT_CLUSTER_BACKEND=k8s-local-kind SCT_CONFIG_FILES=test-cases/scylla-operator/functional.yaml python -m pytest functional_tests/scylla_operator

Reuse cluster
-------------

You can reuse cluster in any mode you are running by populating "SCT_REUSE_CLUSTER" env variable.
There is only difference for local mini kubernetes cluster, in such case it won't respect SCT_REUSE_CLUSTER value
 and will reuse any cluster it find

on EKS::

    SCT_REUSE_CLUSTER=<test_id> SCT_CLUSTER_BACKEND=k8s-eks SCT_CONFIG_FILES=test-cases/scylla-operator/functional.yaml python -m pytest functional_tests/scylla_operator

on Local kind cluster::

    SCT_REUSE_CLUSTER=1 SCT_CLUSTER_BACKEND=k8s-local-kind SCT_CONFIG_FILES=test-cases/scylla-operator/functional.yaml python -m pytest functional_tests/scylla_operator

Using additional pytest options
-------------------------------

It is possible to provide any pytest option to the test runner using `PYTEST_ADDOPTS` env variable.
For example, to make test runner stop after first failure do following::

    PYTEST_ADDOPTS='--maxfail=1' ./sct.py run-pytest functional_tests/scylla_operator ...

Or if it is needed to run tests in random order following can be used::

    # Always random
    PYTEST_ADDOPTS='--random-order' ./sct.py run-pytest functional_tests/scylla_operator ...

    # Keeping seed for specific chain reproducing/debugging
    PYTEST_ADDOPTS='--random-order-seed=12321' ./sct.py run-pytest functional_tests/scylla_operator ...

    # Changing test mixing groups by using --random-order-bucket=module (can also be class, package and global)
    PYTEST_ADDOPTS='--random-order-bucket=module' ./sct.py run-pytest functional_tests/scylla_operator ...

It is possible to provide multiple additional options doing following::

    PYTEST_ADDOPTS='--maxfail=1 --random-order' ./sct.py run-pytest functional_tests/scylla_operator ...

TODO
====

* Set up buildable HTML documentation, and a hosted version of it.
* Write more tests, improve test API (always in progress, I guess).

Known issues
============

* No test API guide. Bear with us while we set up hosted test API documentation, and take a look at the current tests and the `sdcm` library for more information.

Footnotes
=========

* [2] https://ask.fedoraproject.org/en/question/45805/how-to-use-virt-manager-as-a-non-root-user/
* [3] https://prometheus.io/
* [4] http://grafana.org/

.. _repo: https://github.com/scylladb/scylla-monitoring
