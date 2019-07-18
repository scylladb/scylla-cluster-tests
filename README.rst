SCT - Scylla Cluster Tests
##########################

SCT tests are designed to test Scylla database on physical/virtual servers under high read/write load.
Currently the tests are run using avocado[1] framework (version 36.4)
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


From here you can proceed with on of the 2 options

Option 1: Setup SCT in Docker
-----------------------------
As mentioned before, instead of installing all the prerequisites on your machine, you can also use SCT Docker
container (aka Hydra) to run SCT avocado tests::

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

Run a test
----------

Example running avocado using Hydra using `sample.yaml` configuration file

on AWS::

    hydra "avocado --show test run longevity_test.py:LongevityTest.test_custom_time --multiplex tests/sample.yaml --filter-only /run/backends/aws"

on GCE::

    hydra "avocado --show test run longevity_test.py:LongevityTest.test_custom_time --multiplex tests/sample.yaml --filter-only /run/backends/gce"


You can also enter the containerized SCT environment using::

    hydra bash

Depending on which backend hardware/cloud provider/virtualization you will use, relevant configuration of those backend
services should be done.

List resources being used::

    # google cloud engine
    gcloud compute instances list --filter="metadata.items.key['RunByUser']['value']='`whoami`'"

    # amazon
    aws ec2 describe-instances --query 'Reservations[].Instances[].InstanceId' --filter "Name=tag:RunByUser,Values=`whoami`"


Configuring test run configuration YAML
---------------------------------------

Take a look at the ``data_dir/scylla.yaml`` file. It contains a number of
configurable test parameters, such as DB cluster instance types and AMI IDs.
In this example, we're assuming that you have copied ``data_dir/scylla.yaml``
to ``data_dir/your_config.yaml``.

All the test run configurations are stored in ``tests`` directory.

Important: Some tests use custom hardcoded operations due to their nature,
so those tests won't honor what is set in ``data_dir/your_config.yaml``.

Setup Notes - Making your regular user able to access qemu:///session
---------------------------------------------------------------------

You might want to setup libvirt to access the qemu system session as your regular
user. You might want to refer to [3], in case that is not available, here's the
gist of the procedure:

With Fedora 20 onwards, virt-manager implements PolicyKit (I recommend reading the man page). If you want to allow a certain group of users access to virt-manager without providing root credentials, you can create a new rules file in /etc/polkit-1/rules.d and add a rule to permit users who are local, logged in, and in the group you specify (wheel in the example below) access to the virt-manager software::

    sudo vim /etc/polkit-1/rules.d/80-libvirt.rules

And then write::

    polkit.addRule(function(action, subject) {
      if (action.id == "org.libvirt.unix.manage" && subject.local && subject.active && subject.isInGroup("wheel")) {
          return polkit.Result.YES;
      }
    });


Run the tests
=============

AWS - Amazon Web Services
-------------------------

Change your current working directory to this test suite base directory,
then run avocado. Example command line::

    avocado run longevity_test.py:LongevityTest.test_custom_time --multiplex data_dir/your_config.yaml --filter-only /run/backends/aws/us_east_1 /run/databases/scylla --filter-out /run/backends/libvirt /run/backends/openstack /run/backends/gce --open-browser

This command line is to run the test method ``test_custom_time``, in
the class ``Longevitytest``, that lies inside the file ``longevity_test.py``,
and the test will run using the AWS data defined in the branch ``us_east_1``
of ``data_dir/your_config.yaml``. The flag ``--open-browser`` opens the avocado
test job report on your default browser at the end of avocado's execution.


If you want to use the us_west_2 region, you can always change
``/run/regions/us_east_1`` to ``/run/regions/us_west_2`` in
the command above. You can also change the value ``/run/databases/scylla`` bit
to ``/run/databases/cassandra`` to run the same test on a cassandra node.

Also, please note that ``scylla.yaml`` is a sample configuration.
On your organization, you really have to update values with ones you
actually have access to.

You'll see something like::

    JOB ID     : ca47ccbaa292c4d414e08f2167c41776f5c3da61
    JOB LOG    : /home/lmr/avocado/job-results/job-2016-01-05T20.45-ca47ccb/job.log
    TESTS      : 1
     (1/1) longevity_test.py:LongevityTest.test_custom_time : /

A throbber, that will spin until the test ends. This will hopefully evolve to::

    JOB ID     : ca47ccbaa292c4d414e08f2167c41776f5c3da61
    JOB LOG    : /home/lmr/avocado/job-results/job-2016-01-05T20.45-ca47ccb/job.log
    TESTS      : 1
     (1/1) longevity_test.py:LongevityTest.test_custom_time : PASS (1083.19 s)
    RESULTS    : PASS 1 | ERROR 0 | FAIL 0 | SKIP 0 | WARN 0 | INTERRUPT 0
    JOB HTML   : /home/lmr/avocado/job-results/job-2016-01-05T20.45-ca47ccb/html/results.html
    TIME       : 1083.19 s

Reuse Cluster (AWS)
^^^^^^^^^^^^^^^^^^^

Running a test with already provisioned cluster, you can get the test_id in the AWS console of the one of the nodes tags tab::

    # add the following to your config yaml
    test_id: 7c86f6de-f87d-45a8-9e7f-61fe7b7dbe84
    reuse_cluster: true

    # or with using the new configuration, before the run test command
    export SCT_TEST_ID=7c86f6de-f87d-45a8-9e7f-61fe7b7dbe84
    export SCT_REUSE_CLUSTER=true


Libvirt
-------

In order to run tests using the libvirt backend, you'll need:

1. One qcow2 base image with CentOS 7 installed. This image needs to have a user
   named 'centos', and this user needs to be configured to not require a password
   when running commands with sudo.

2. `cp data_dir/scylla.yaml data_dir/your_config.yaml`

3. Edit the configuration file (data_dir/your_config.yaml) to add the path to
   the CentOS image mentioned on step 1, as well as tweak values present in the
   `libvirt:` session of that file. One of the values you might want to tweak is
   the scylla yum repository used to install scylla on the CentOS 7 VM.

With that said and done, you can run your test using the command line::

    avocado run longevity_test.py:LongevityTest.test_custom_time --multiplex data_dir/scylla-lmr.yaml --filter-only /run/backends/libvirt /run/databases/scylla --filter-out /run/backends/aws /run/backends/openstack /run/backends/gce --open-browser


OpenStack
---------

In order to run tests using the openstack backend, you'll need:

1. A deployed OpenStack lab
2. One CentOS 7 image. This image needs to have a user
   named 'centos', and this user needs to be configured to not require a password
   when running commands with sudo.

3. `cp data_dir/scylla.yaml data_dir/your_config.yaml`

4. Edit the configuration file (data_dir/your_config.yaml) to tweak values present
   in the `openstack:` session of that file. One of the values you might want to
   tweak is the scylla yum repository used to install scylla on the CentOS 7 image.

With that said and done, you can run your test using the command line::

    avocado run longevity_test.py:LongevityTest.test_custom_time --multiplex data_dir/scylla-lmr.yaml --filter-only /run/backends/libvirt /run/databases/scylla --filter-out /run/backends/aws /run/backends/libvirt /run/backends/gce --open-browser

GCE - Google Compute Engine
---------------------------

In order to run tests using the GCE backend, you'll need:

1. A GCE account

2. `cp data_dir/scylla.yaml data_dir/your_config.yaml`

3. Edit the configuration file (data_dir/your_config.yaml) to tweak values present
   in the `gce:` session of that file. One of the values you might want to
   tweak is the scylla yum repository used to install scylla on the CentOS 7 image.

With that said and done, you can run your test using the command line::

    avocado run longevity_test.py:LongevityTest.test_custom_time --multiplex data_dir/scylla-lmr.yaml --filter-only /run/backends/libvirt /run/databases/scylla --filter-out /run/backends/aws /run/backends/libvirt /run/backends/openstack --open-browser


(Optional) Follow what the test is doing
========================================

What you can do while the test is running to see what's happening::

    tail -f ~/avocado/job-results/latest/job.log

or::

    tail -f ~/avocado/job-results/latest/test-results/longevity_test.py\:LongevityTest.test_custom_time/debug.log

At the end of the test, there's a path to an HTML file with the job report.
The flag ``--open-browser`` actually opens that at the end of the test.

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

    15:47:55 INFO | [54.193.84.90] Running '/usr/bin/ssh -a -x  -o ControlPath=/var/tmp/ssh-masterTQ3hZu/socket -o StrictHostKeyChecking=no -o UserKnownHostsFile=/var/tmp/tmpOjFA9Q -o BatchMode=yes -o ConnectTimeout=300 -o ServerAliveInterval=300 -l centos -p 22 -i /var/tmp/lmr-longevity-test-8b95682d.pem 54.193.84.90 "cassandra-stress write cl=QUORUM duration=30m -schema 'replication(factor=3)' -port jmx=6868 -mode cql3 native -rate threads=4 -node 172.31.18.109"'
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
on per node files, you can find them all in the test log directory (the
avocado HTML report will help you locate and visualize all those files, just
click on the test name link and you'll see the dir structure.

SCT utilities
=============

1) utils/fetch_and_decode_stalls_from_job_database_logs.sh

This script searches in the log all reactor stalles, find unique stalles and decode them.
The script analyzes the database.logs that are located under avocado/job-results/<job-folder>/<test-folder>/<cluster-folder>. The script is going through nodes folders and analyze database.log for every node.

2) utils/fetch_and_decode_stalls_from_journalctl_logs_all_nodes.sh -

This script searches in the journalctl all reactor stalles, find unique stalles and decode them.
Save the journalctl from every node to the database.log and move to the folders by node. Organize all folders in one folder, like::

            logs
        |            |
    node1          node2
    database.log   database.log

3) utils/fetch_and_decode_stalls_from_one_journalctl_log.sh

This script searches in the one journalctl all reactor stalles, find unique stalles and decode them.
Save the journalctl of one node to the database.log and move to the folder

For examples see utilities documentation

TODO
====

* Set up buildable HTML documentation, and a hosted version of it.
* Write more tests, improve test API (always in progress, I guess).

Known issues
=============

* No test API guide. Bear with us while we set up hosted test API documentation, and take a look at the current tests and the `sdcm` library for more information.

Footnotes
=========

* [1] http://avocado-framework.github.io/
* [2] https://ask.fedoraproject.org/en/question/45805/how-to-use-virt-manager-as-a-non-root-user/
* [3] https://prometheus.io/
* [4] http://grafana.org/

.. _repo: https://github.com/scylladb/scylla-monitoring
