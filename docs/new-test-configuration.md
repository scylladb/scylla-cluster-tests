# Refactoring of scylla-cluster-tests configuration files

## Key goals
1) Trying to flatten the config files, i.e. no nested data structures in the config
2) multiple test configs files can passed via environment variables (to bypass avocado command line for now) `SCT_CONFIG_FILES='["tests/longevity-50GB-4days.yaml","overwrites.yaml"]'`
3) using [python-anyconfig](https://github.com/ssato/python-anyconfig) - That would enable us to support toml/yaml/json and also have a builtin to merge multiple configuration files

## Directory structure
```bash
# several default config files, that are taken first
/defaults
|-- aws_config.yaml
|-- gce_config.yaml
|-- azure_config.yaml
|-- docker_config.yaml
|-- libvirt_config.yaml

# test case files which are taken from command line
/tests
|-- testcase_logevity_1TB-7days.yaml
|-- testcase_logevity_50Gb-4days.yaml

```

## Example of usage
```bash
export SCT_NEW_CONFIG=yes # temporary, so we can still work with the old config yamls

export SCT_TEST_DURATION=600 # overwrite from environment variables
export SCT_CLUSTER_BACKEND=aws # including backend selection, we can decide on the default backend, for example docker one
export SCT_CONFIG_FILES='["tests/longevity-50GB-4days.yaml","overwrites.ini"]'

avocado --show test run longevity_test.py:LongevityTest.test_custom_time
```

## Validation
Having one place we define all the available configs, their help test and defaults
(similar to argparse api, https://docs.python.org/3/library/argparse.html)
````python
config_option = [
    dict(name="cluster_backend", env="SCT_CLUSTER_BACKEND", help="", default="docker", type=str, required=True),
    dict(name="test_duration", end="SCT_TEST_DURATION", help="", default=600, type=int, requried=True),
    ...
]

# those can be added to a json scheme to validate / or write the validation code for it to be a bit clearer output
aws_required_params = ["instance_type_loader", "instance_type_monitor", "instance_type_db", "instance_type_db", 
                       "region_name", "security_group_ids", "subnet_id", "ami_id_db_scylla", "ami_id_loader", 
                       "ami_id_monitor", "aws_root_disk_size_monitor", "ami_db_scylla_user", "ami_monitor_user"]

````

## Test cases declared config usage

TODO: think of a way for test to declare which parameters are mandatory for them and check them

```python
    def test_custom_time(self):
        """
        Run cassandra-stress with params defined in data_dir/scylla.yaml
        """
        
        self.config.assert_configuration(['prepare_write_cmd', 'keyspace_num', 'pre_create_schema', 'nemesis_interval'])
        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(),
                                    tester_obj=self)
```
