# scylla-cluster-tests pipelines

## Goals
1. Pipeline should be reusable as much as possible
2. All of the data should be inside sct code base, so it can be reviewed
3. Some pipelines should be parallels (i.e. rolling upgrades, artifacts)
4. we should have one pipeline for unit-tests of sct itself, which run as part of each PR (which nowdays called provision tests)
5. Some values should be hardcoded, and can't be overwritten when running the pipelines

# Directory Structure/Layout

SCT directory structure should match exactly release directory

Readable name for folders would be a mapping on the code

Enterprise features would be under a unique folder

![Overview](./sct_pipelines.png?raw=true "Directory")

### TODOs:
- [ ] arrange the pipelines as folders
- [ ] fix the generation code to create the job in jenkins
- [ ] manually move master/enterprise jobs (if we want to save history, is argus o.k. with moving jobs ?)
- [ ] fix the triggers all over the place to match

## longevity-10gb-3h.jenkinsfile

```groovy
longevityPipeline(
    backend: 'aws',
    region: 'eu-west-1',
    test_name: 'longevity_test.py:LongevityTest.test_custom_time',
    test_config: 'test-cases/longevity/longevity-10gb-3h.yaml',
)
```

## Running on a local Jenkins agent

By default a pipeline schedules on the region-appropriate cloud builder
(`getJenkinsLabels` maps backend+region to an ASG/template label), and longevity-style
pipelines then provision a cloud sct-runner that the test actually runs on. Two knobs change
that, both usable independently:

- **`jenkins_label` (job parameter, any backend, any pipeline that declares it)** - pin the
  build to a named agent label, bypassing the region→builder mapping entirely. Empty (the
  default) keeps the normal mapping, so build #1 - where parameters are not yet populated -
  behaves exactly as before. Use it to debug on a specific machine or to prove out a new
  agent before giving it a real label.

- **`local_agent: true` (jenkinsfile literal, longevity / rolling-upgrade)** - skip the
  *Create SCT Runner* / *Clean SCT Runners* stages and run the test right on the Jenkins
  agent, the way `artifactsPipeline` always has (every stage helper already takes the
  builder-local path when `./sct_runner_ip` is absent). It must be combined with something
  that picks an appropriate agent - `minicloud: true` (which resolves the KVM-capable label)
  or a `jenkins_label`.

Why a jenkinsfile literal and not a job parameter: the agent label is decided at
pipeline-definition time, before parameters exist on build #1 - the same reason
`functional_test` works the way it does.

For minicloud specifically - which needs a KVM-capable agent and mandates the local topology
on artifacts - see [minicloud](./minicloud.md). For the cloud sct-runner mechanics, see
[sct-runners](./sct-runners.md).
