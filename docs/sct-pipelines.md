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
