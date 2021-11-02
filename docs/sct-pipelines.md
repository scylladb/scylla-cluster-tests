# scylla-cluster-tests pipelines

## Goals
1. Pipeline should be reusable as much as possible
2. All of the data should be inside sct code base, so it can be reviewed
3. Some pipelines should be parallels (i.e. rolling upgrades)
4. we should have one pipeline for unit-tests of sct itself, which run as part of each PR
5. Some values should be hardcoded, and can't be overwritten when running the pipelines

# Directory Structure/Layout

![Overview](./sct_pipelines.png?raw=true "Directory")

## master-longevity-10gb-3h.jenkinsfile

```groovy
longevityPipeline(
    region: 'eu-west-1',
    test_name: 'longevity_test.py:LongevityTest.test_custom_time',
    test_config: 'test-cases/longevity/longevity-10gb-3h.yaml',

    timeout: [time: 200, unit: 'MINUTES']
)
```

### TODOs:
- [ ] make a script to create/recreate a branch jobs as needed
- [ ] jenkins workers need better tags, pre region or cloud provider


### Linting Jenkinsfile
```
npm install jflint

nano ~/.jflintrc
# add you cred and api token

#{
#  "jenkinsUrl": "http://jenkins.scylladb.com",
#  "username": "fruch",
#  "password": "[replace with your api key]"
#}

./node_modules/.bin/jflint Jenkinsfile
```
