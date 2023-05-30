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
