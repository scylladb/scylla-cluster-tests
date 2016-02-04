Scylla Cluster Tests
====================

Here you can find some avocado [1] tests for scylla clusters.
Those tests can automatically create a scylla cluster, some loader machines
and then run operations defined by the test writers, such as database
workloads.

What's inside?
--------------

1. A library, called ``sdcm`` (stands for scylla distributed cluster
   manager). The word 'distributed' was used here to differentiate
   between that and CCM, since with CCM the cluster nodes are usually
   local processes running on the local machine instead of using actual
   machines running scylla services as cluster nodes. It contains:

   * ``sdcm.cluster``: Cluster classes that use the ``boto3`` API [2]
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

3. Python files with tests. The convention is that test files have the ``_test.py`` suffix.

Setup
-----

Install ``boto3`` and ``awscli`` (the last one is to help you configure aws)::

    sudo -H pip install boto3
    sudo -H pip install awscli

Install avocado: http://avocado-framework.readthedocs.org/en/latest/GetStartedGuide.html#installing-avocado

Due to a bug in avocado 0.32 that is going to be fixed for the next release,
you also have to install the html plugin if you are on Fedora::

    sudo dnf install avocado-plugins-output-html -y

Configure aws::

    aws configure

That will ask you for your ``region``, ``aws_access_key_id``,
``aws_secret_access_key``. Please complete that.

Take a look at the ``data_dir/scylla.yaml`` file. It contains a number of
configurable test parameters, such as DB cluster instance types and AMI IDs.
In this example, we're assuming that you have copied ``data_dir/scylla.yaml``
to ``data_dir/your_config.yaml``.

Important: Some tests use custom hardcoded operations due to their nature,
so those tests won't honor what is set in ``data_dir/your_config.yaml``.

Run the tests
-------------

Change your current working directory to this test suite base directory,
then run avocado. Example command line::

    avocado run longevity_test.py:LongevityTest.test_custom_time --multiplex data_dir/your_config.yaml --filter-only /run/regions/us_east_1 /run/databases/scylla --open-browser

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

(Optional) Follow what the test is doing
----------------------------------------

What you can do while the test is running to see what's happening::

    tail -f ~/avocado/job-results/latest/job.log

or::

    tail -f ~/avocado/job-results/latest/test-results/longevity_test.py\:LongevityTest.test_custom_time/debug.log

At the end of the test, there's a path to an HTML file with the job report.
The flag ``--open-browser`` actually opens that at the end of the test.

TODO
----

* Set up buildable HTML documentation, and a hosted version of it.
* Writing more tests, improving the test API.
* Allowing the use of more backends, such as libvirt vms, as an alternative to AWS.

Known issues
------------

* No test API guide. Bear with us while we set up hosted test API documentation, and take a look at the current tests and the `sdcm` library for more information.

Footnotes
---------

* [1] http://avocado-framework.github.io/
* [2] http://aws.amazon.com/sdk-for-python/
