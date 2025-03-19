# Moving to docker based loaders

### Current situation

Currently, have fixed images for AWS, installing from repo/list in GCE/Azure
Into those images we install enterprise cassandra-stress, so it would cover
all the functionality needed

Rest of the stress/load tools are installed during setup of the loader nodes (scylla-bench, cassandra-harry, gemini),
which take time to install/compile

ycsb, ndbench, nosqlbench, cdcreader and kcl are using `DockerBasedStressThread`, and served as a POC for the idea
of using docker, working for quite a long time without any specific to docker issues.

### Why

We want to align all the loaders to be docker based, for the following reasons:
1) speed up the startup of setup of loaders
2) be able to run multiple versions of loaders at the same run
3) better fit k8s operation for testing scylla-operator, and have pod (or multiple of them) per stress tool as needed
4) stop manually maintaining images across multiple regions and cloud vendors

### Images

For most of the tools we are going to create our docker images, under `scylladb/hydra-loaders` and with
instructions on building each one in `/docker`, we should version them by the tool version if applicable, or by date.
the only tool we are not going to build ourselves is cassandra-stress, which would be comping from the official docker images
of scylladb.

### Finding correct cassandra-stress image

Since we are going to be using scylla docker images for loaders, we need a way to find the correct docker image, based on a
given version or sha of scylla (both for OSS and Enterprise versions), if we'll find that match base on sha would be problematic
we'll fall back to last available release (smaller than the currently under test version, ex. when testing 5.2-dev, we'll take 5.1.x version)

### Configuration

We should be able to override the default in the test configuration:

```yaml
stress_image:
  ycsb: scylladb/hydra-loaders:ycsb-jdk8-20211104
  cassandra-stress: scylladb/scylla:5.0.0
  scylla-bench: scylladb/scylla-bench:0.2.0
```

or via environment variables :

```bash
export SCT_STRESS_IMAGE='{"ycsb": "scylladb/hydra-loaders:ycsb-jdk8-20211104"}'
export SCT_STRESS_IMAGE.cassandra-stress="scylladb/scylla:5.0.0"
export SCT_STRESS_IMAGE.scylla-bench="scylladb/scylla-bench:0.2.0"
```

for using multiple versions, we should be able to define the image as part of the stress command:

```yaml
# first would use version 4.6.5, and the 2nd would use the default
stress_cmd: ["image=scylladb/scylla:4.6.5; cassandra-stress write cl=QUORUM duration=180m ... -rate threads=1000 -pop seq=1..10000000 -log interval=5"
             "cassandra-stress write cl=QUORUM duration=180m ... -rate threads=1000 -pop seq=1..10000000 -log interval=5"]
```

this is for cases we might have specific bugs in a nemesis for example that would prevent us from using latest scylla-bench, or latest cassandra-stress
could be added once we encounter those specific requirements

### Concerns

* Performance tests might have slight different behaviors/timing when working under docker:
  we should strive to using the host networking, and pin enough CPUs to each stress threads running
  we will need to run few candidates of the Performance tests, with/without those changes, to evaluate.

* for K8S setup we're already using the same image as the image under tests, but switch scylla-bench to docker for example,
  would mean we'll need to refactor the way create loader pods (since in the code, we'll end up with a docker running
  from within a pod, and not part of the k8s control), so we might handle scylla-bench last until we'll
  have clearer design for k8s loader
