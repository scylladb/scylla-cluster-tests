
```bash

hydra run test_longevity.py --conf longevity-1tb.yaml

hydra conf longevity-1tb.yaml

hydra provision --backend aws --scylla-version 3.0
hydra provision --backend aws --db-nodes 3 --loaders 1 --scylla-version 3.0 --monitoring-version 2.1

hydra list-resources --test-id n3vik6-ssu84ld --user bentsi

hydra clean-resources --test-id n3vik6-ssu84ld --backend aws
hydra clean-resources --backend gce --user bentsi
```