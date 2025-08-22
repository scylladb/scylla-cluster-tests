
```bash
# run a test
hydra run-test longevity_test.LongevityTest.test_custom_time --backend aws --config test-cases/sample.yaml

# show test configuration in yaml format:
hydra output-conf unit_tests/test_configs/minimal_test_case.yaml -b gce

# check if test config is o.k.

hydra conf unit_tests/test_configs/minimal_test_case.yaml
# check test config for specific backend
hydra conf unit_tests/test_configs/minimal_test_case.yaml --backend gce

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

# get Scylla base versions for upgrade test

hydra get-scylla-base-versions --only-print-versions true --linux-distro centos --scylla-repo http://downloads.scylladb.com/unstable/scylla/master/rpm/centos/2021-08-29T00:58:58Z/scylla.repo

```
