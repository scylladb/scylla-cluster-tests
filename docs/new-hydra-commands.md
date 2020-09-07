
```bash
# run a test will all the regular avocado params
hydra run longevity_test.py:LongevityTest.test_custom_time --multiplex tests/sample.yaml --filter-only /run/backends/aws/us_east_1 --filter-out /run/backends/gce

# show test configuration in yaml format:
hydra output-conf internal_test_data/minimal_test_case.yaml -b gce

# check if test config is o.k.

hydra conf internal_test_data/minimal_test_case.yaml
# check test config for specific backend
hydra conf internal_test_data/minimal_test_case.yaml --backend gce

# listing resource used in cloud (AWS/GCE)
hydra list-resources --test-id n3vik6-ssu84ld --user bentsi


# cleanup resources
hydra clean-resources --post-behavior
hydra clean-resources --post-behavior --logdir /path/to/logdir
hydra clean-resources --test-id n3vik6-ssu84ld
hydra clean-resources --user bentsi
hydra clean-resources --dry-run

# WIP: provision a cluster without running any test scenario i.e. stress/nemesis
hydra provision --backend aws --scylla-version 3.0
hydra provision --backend aws --db-nodes 3 --loaders 1 --scylla-version 3.0 --monitoring-version 2.1
```
