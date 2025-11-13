# SCT Runner

A machine on any of the cloud, with a known fixed image,
that we are using to run the test on, so the test run in a local
network environment of the cluster under test.

All our jenkins job are those runners for the test

For development purpose one can create he's own test machine
and run hydra on it:

```bash
hydra create-runner-instance --cloud-provider <cloud_name> -r <region_name> -z <az> -t <test-id> -d <run_duration>

# run with specific address of a runner
hydra --execute-on-runner [runner ip] [Any hydra commands]

# `create-runner-instance` command should save the address into a file
hydra --execute-on-runner `cat sct_runner_ip` [Any hydra commands]
```

hydra is syncing the whole SCT directory with rsync,
try to make sure there's no huge files in SCT directory,
because that would slow it down


### process of updating sct runner images

1. update code in sct_runner.py, give it a new version number
2. Build all images
    ```bash

     ./sct.py create-runner-image -c aws -r eu-west-2 -z a

    ./sct.py create-runner-image -c gce -r us-east1 -z a
    SCT_GCE_PROJECT=gcp-local-ssd-latency ./sct.py create-runner-image -c gce -r us-east1 -z a

    ./sct.py create-runner-image -c azure -r eastus -z a
    ```
3. update version on `aws_builder.py` and `gce_builder.py`
4. build jenkins configuration with new sct runner image
    ```bash
    ./sct.py configure-jenkins-builders -c gce
    ./sct.py configure-jenkins-builders -c aws
    ```

5. update `vars/getJenkinsLabels.groovy` with the new labels created in step 1
