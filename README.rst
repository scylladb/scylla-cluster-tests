Scylla Longevity Tests
======================

Here you can find some avocado [1] tests for scylla longevity tests
(we'll automatically create a scylla cluster, loader machines and then
run it for a long(er) period of time).

What's inside?
--------------

1) A library, called sdcm (stands for scylla distributed cluster
manager). The word 'distributed' was used here to differentiate
between that and CCM, since with CCM the cluster nodes are usually
local processes running on the local machine instead of actual
machines on the network. It's probably not great, but I had to choose
something. It contains:
* `sdcm.cluster`: Cluster classes that use the boto3 API
* `sdcm.remote`: SSH library
* `sdcm.nemesis`: Nemesis classes (a nemesis is a class that does
disruption in the node)
* `sdcm.tester`: Contains the base test class, see below.

2) A directory, named scylla_longevity.py.data. that contains:
* scylla repo file (to prepare a loader node)
* yaml file containing test data
- AWS machine image ids
- Security groups
- Number of loader nodes (gotta keep those at 1 due to one current
limitation of my code [1])
- Number of cluster nodes (I'm choosing 6 right now, but I see
cluster problems when the test tries to stop one node [2])
3) A test, located at scylla_longevity.py.

Setup
-----

* Install boto3 and awscli (the last one is to help you configure aws)::

    sudo -H pip install boto3
    sudo -H pip install awscli

* Install avocado::

    http://avocado-framework.readthedocs.org/en/latest/GetStartedGuide.html#installing-avocado

* Configure aws::

    aws configure

That will ask you for your region, aws_access_key_id,
aws_secret_access_key. Please complete that.

* Make sure you set PYTHONPATH to include the directory the class is in [3]:

scylla-longevity-tests $ export PYTHONPATH=.:$PYTHONPATH

* Run avocado::

    avocado run scylla_longevity.py:LongevityTest.test_20_minutes --multiplex scylla_longevity.py.data/scylla-longevity.yaml --filter-only /run/regions/us_east_1

This command line is just for the short version, using us_east_1. If you want
to use the us_west_2 region, you can always change the string in the command
above.

Also, please note that this is a sample configuration. On your organization,
you really have to update values with ones you actually have access to.

You'll see something like::

    JOB ID     : ca47ccbaa292c4d414e08f2167c41776f5c3da61
    JOB LOG    : /home/lmr/avocado/job-results/job-2016-01-05T20.45-ca47ccb/job.log
    TESTS      : 1
     (1/1) scylla_longevity.py:LongevityTest.test_20_minutes : /

A throbber, that will spin until the test ends. This will hopefully evolve to::

    avocado run scylla_longevity.py --multiplex
    scylla_longevity.py.data/scylla-longevity.yaml
    JOB ID     : ca47ccbaa292c4d414e08f2167c41776f5c3da61
    JOB LOG    : /home/lmr/avocado/job-results/job-2016-01-05T20.45-ca47ccb/job.log
    TESTS      : 1
     (1/1) scylla_longevity.py:LongevityTest.test_20_minutes: PASS
    (1083.19 s)
    RESULTS    : PASS 1 | ERROR 0 | FAIL 0 | SKIP 0 | WARN 0 | INTERRUPT 0
    JOB HTML   : /home/lmr/avocado/job-results/job-2016-01-05T20.45-ca47ccb/html/results.html
    TIME       : 1083.19 s

What you can do while the test is running to see what's happening:

* tail the job.log file (lots of debug info as I told you)
* tail the test stdout:
~/avocado/job-results/latest/test-results/scylla_longevity.py\:LongevityTest.test_twenty_minutes/stdout
* At the end of the test, there's a path to an HTML file with the job report.

Known issues
------------

* SSH paralelism issue - forces using 1 loader instead of > 1
* The avocado log is verbose - it captures all debug output of the
boto API. I still need to see if I can fix that.
* cassandra-stress does not return an error code on exit. This means
we have to manually inspect its output instead of relying an error
code. I'll do that later.

Footnotes
---------

[1] http://avocado-framework.github.io/
