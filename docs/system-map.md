# SCT system map

What a test run is made of on each backend, drawn for the Longevity shape. Expand a backend to see its diagram and the test types it supports.

<details><summary><b>AWS</b> · AWS account</summary>

```mermaid
flowchart LR
  dev[Developer / Jenkins] --> runner
  subgraph runner["SCT runner"]
    test[SCT test - Python]
  end
  subgraph env["AWS account"]
    db[("6 × EC2 · Scylla AMI")]
    ld["2 × EC2 · Ubuntu"]
    mon["EC2 · monitor AMI"]
  end
  runner -->|"EC2 API"| env
  test -->|stress commands| ld
  ld -->|CQL| db
  test -.->|nemesis| db
  mon -.->|scrape| db
  env --- s0["Amazon EC2 AMIs"]
  env --- s1["EC2 Spot Instances"]
  test -->|results · errors| argus[(Argus)]
```

| Test type | What it does |
|---|---|
| [Longevity](../docs/longevity.md) | Long-running mixed load with chaos. The default shape most tests share. |
| [Performance regression](../docs/performance/README.md) | Steady stress against a fixed cluster. HDR latency and throughput go to Argus, which compares with past runs and fails on regression. |
| [Rolling upgrade](../docs/rolling-upgrade.md) | Starts on a base version and upgrades one node at a time under load, optionally rolling some back. |
| [Artifacts](../docs/artifacts_test.md) | Checks a package, image or AMI on one node. No loaders, no monitor. |
| [Scylla Manager](../docs/configuration_options/scylla-manager.md) | Backup, restore, repair and health checks driven by sctool. Backups go to the backend's object storage. |
| [Multi-DC longevity](../docs/longevity.md) | One cluster across regions. List values give per-DC counts; rack_aware_loader keeps load local. |
| [Gemini](https://github.com/scylladb/gemini#readme) | Gemini sends the same random operations to the test cluster and an oracle cluster and fails on any difference. Nemesis hits the test cluster only. |
| [Alternator](../docs/configuration_options/alternator-dynamodb-api.md) | Same topology as longevity; the load uses the DynamoDB-compatible API instead of CQL. |
| [Cassandra → Scylla (Spark/EMR)](../docs/cs-to-scylla-migration.md) | An EMR Spark job runs scylla-migrator from a Cassandra source cluster into Scylla, then a validator job. |
| [Vector search](../docs/configuration_options/vector-store.md) | Vector-store nodes attached to the Scylla cluster, serving ANN queries over HTTP. |
| [Kafka connectors](../docs/kafka.md) | Kafka runs as a docker-compose stack on the SCT runner. CDC source and sink connectors move data between Scylla and Kafka. |
| [Security: EaR + LDAP](../test-cases/longevity) | Encryption at rest with the cloud KMS (key rotation threads), and LDAP auth against an OpenLDAP container on the runner. |

</details>

<details><summary><b>GCE</b> · GCP project</summary>

```mermaid
flowchart LR
  dev[Developer / Jenkins] --> runner
  subgraph runner["SCT runner"]
    test[SCT test - Python]
  end
  subgraph env["GCP project"]
    db[("6 × GCE VM · Scylla image")]
    ld["2 × GCE VM · Ubuntu"]
    mon["GCE VM · monitor image"]
  end
  runner -->|"Compute Engine API"| env
  test -->|stress commands| ld
  ld -->|CQL| db
  test -.->|nemesis| db
  mon -.->|scrape| db
  env --- s0["GCE images"]
  env --- s1["Spot VMs"]
  test -->|results · errors| argus[(Argus)]
```

| Test type | What it does |
|---|---|
| [Longevity](../docs/longevity.md) | Long-running mixed load with chaos. The default shape most tests share. |
| [Performance regression](../docs/performance/README.md) | Steady stress against a fixed cluster. HDR latency and throughput go to Argus, which compares with past runs and fails on regression. |
| [Rolling upgrade](../docs/rolling-upgrade.md) | Starts on a base version and upgrades one node at a time under load, optionally rolling some back. |
| [Artifacts](../docs/artifacts_test.md) | Checks a package, image or AMI on one node. No loaders, no monitor. |
| [Scylla Manager](../docs/configuration_options/scylla-manager.md) | Backup, restore, repair and health checks driven by sctool. Backups go to the backend's object storage. |
| [Multi-DC longevity](../docs/longevity.md) | One cluster across regions. List values give per-DC counts; rack_aware_loader keeps load local. |
| [Gemini](https://github.com/scylladb/gemini#readme) | Gemini sends the same random operations to the test cluster and an oracle cluster and fails on any difference. Nemesis hits the test cluster only. |
| [Alternator](../docs/configuration_options/alternator-dynamodb-api.md) | Same topology as longevity; the load uses the DynamoDB-compatible API instead of CQL. |
| [Jepsen](../docs/configuration_options/jepsen-tests.md) | Jepsen consistency tests. The control node drives the DB nodes over SSH and injects its own faults. |
| [Kafka connectors](../docs/kafka.md) | Kafka runs as a docker-compose stack on the SCT runner. CDC source and sink connectors move data between Scylla and Kafka. |
| [Security: EaR + LDAP](../test-cases/longevity) | Encryption at rest with the cloud KMS (key rotation threads), and LDAP auth against an OpenLDAP container on the runner. |

</details>

<details><summary><b>Azure</b> · Azure subscription</summary>

```mermaid
flowchart LR
  dev[Developer / Jenkins] --> runner
  subgraph runner["SCT runner"]
    test[SCT test - Python]
  end
  subgraph env["Azure subscription"]
    db[("6 × Azure VM · Scylla image")]
    ld["2 × Azure VM · Ubuntu"]
    mon["Azure VM · Ubuntu"]
  end
  runner -->|"Azure Resource Manager"| env
  test -->|stress commands| ld
  ld -->|CQL| db
  test -.->|nemesis| db
  mon -.->|scrape| db
  env --- s0["Shared Image Gallery"]
  env --- s1["Azure Spot Virtual Machines"]
  test -->|results · errors| argus[(Argus)]
```

| Test type | What it does |
|---|---|
| [Longevity](../docs/longevity.md) | Long-running mixed load with chaos. The default shape most tests share. |
| [Performance regression](../docs/performance/README.md) | Steady stress against a fixed cluster. HDR latency and throughput go to Argus, which compares with past runs and fails on regression. |
| [Rolling upgrade](../docs/rolling-upgrade.md) | Starts on a base version and upgrades one node at a time under load, optionally rolling some back. |
| [Artifacts](../docs/artifacts_test.md) | Checks a package, image or AMI on one node. No loaders, no monitor. |
| [Scylla Manager](../docs/configuration_options/scylla-manager.md) | Backup, restore, repair and health checks driven by sctool. Backups go to the backend's object storage. |
| [Multi-DC longevity](../docs/longevity.md) | One cluster across regions. List values give per-DC counts; rack_aware_loader keeps load local. |
| [Gemini](https://github.com/scylladb/gemini#readme) | Gemini sends the same random operations to the test cluster and an oracle cluster and fails on any difference. Nemesis hits the test cluster only. |
| [Alternator](../docs/configuration_options/alternator-dynamodb-api.md) | Same topology as longevity; the load uses the DynamoDB-compatible API instead of CQL. |
| [Kafka connectors](../docs/kafka.md) | Kafka runs as a docker-compose stack on the SCT runner. CDC source and sink connectors move data between Scylla and Kafka. |
| [Security: EaR + LDAP](../test-cases/longevity) | Encryption at rest with the cloud KMS (key rotation threads), and LDAP auth against an OpenLDAP container on the runner. |

</details>

<details><summary><b>OCI</b> · OCI tenancy</summary>

```mermaid
flowchart LR
  dev[Developer / Jenkins] --> runner
  subgraph runner["SCT runner"]
    test[SCT test - Python]
  end
  subgraph env["OCI tenancy"]
    db[("6 × OCI instance · DenseIO")]
    ld["2 × OCI instance · Ubuntu"]
    mon["OCI instance · Ubuntu"]
  end
  runner -->|"OCI Core API"| env
  test -->|stress commands| ld
  ld -->|CQL| db
  test -.->|nemesis| db
  mon -.->|scrape| db
  env --- s0["Images · Object Storage"]
  env --- s1["On-demand instances"]
  test -->|results · errors| argus[(Argus)]
```

| Test type | What it does |
|---|---|
| [Longevity](../docs/longevity.md) | Long-running mixed load with chaos. The default shape most tests share. |
| [Performance regression](../docs/performance/README.md) | Steady stress against a fixed cluster. HDR latency and throughput go to Argus, which compares with past runs and fails on regression. |
| [Rolling upgrade](../docs/rolling-upgrade.md) | Starts on a base version and upgrades one node at a time under load, optionally rolling some back. |
| [Artifacts](../docs/artifacts_test.md) | Checks a package, image or AMI on one node. No loaders, no monitor. |
| [Scylla Manager](../docs/configuration_options/scylla-manager.md) | Backup, restore, repair and health checks driven by sctool. Backups go to the backend's object storage. |
| [Multi-DC longevity](../docs/longevity.md) | One cluster across regions. List values give per-DC counts; rack_aware_loader keeps load local. |
| [Gemini](https://github.com/scylladb/gemini#readme) | Gemini sends the same random operations to the test cluster and an oracle cluster and fails on any difference. Nemesis hits the test cluster only. |
| [Alternator](../docs/configuration_options/alternator-dynamodb-api.md) | Same topology as longevity; the load uses the DynamoDB-compatible API instead of CQL. |
| [Kafka connectors](../docs/kafka.md) | Kafka runs as a docker-compose stack on the SCT runner. CDC source and sink connectors move data between Scylla and Kafka. |
| [Security: EaR + LDAP](../test-cases/longevity) | Encryption at rest with the cloud KMS (key rotation threads), and LDAP auth against an OpenLDAP container on the runner. |

</details>

<details><summary><b>Docker</b> · Docker host</summary>

```mermaid
flowchart LR
  dev[Developer / Jenkins] --> runner
  subgraph runner["Hydra on the host"]
    test[SCT test - Python]
  end
  subgraph env["Docker host"]
    db[("6 × container · scylladb/scylla")]
    ld["2 × stress containers"]
    mon["monitoring containers"]
  end
  runner -->|"docker daemon"| env
  test -->|stress commands| ld
  ld -->|CQL| db
  test -.->|nemesis| db
  mon -.->|scrape| db
  test -->|results · errors| argus[(Argus)]
```

| Test type | What it does |
|---|---|
| [Longevity](../docs/longevity.md) | Long-running mixed load with chaos. The default shape most tests share. |
| [Rolling upgrade](../docs/rolling-upgrade.md) | Starts on a base version and upgrades one node at a time under load, optionally rolling some back. |
| [Artifacts](../docs/artifacts_test.md) | Checks a package, image or AMI on one node. No loaders, no monitor. |
| [Alternator](../docs/configuration_options/alternator-dynamodb-api.md) | Same topology as longevity; the load uses the DynamoDB-compatible API instead of CQL. |
| [Vector search](../docs/configuration_options/vector-store.md) | Vector-store nodes attached to the Scylla cluster, serving ANN queries over HTTP. |
| [Kafka connectors](../docs/kafka.md) | Kafka runs as a docker-compose stack on the SCT runner. CDC source and sink connectors move data between Scylla and Kafka. |

</details>

<details><summary><b>EKS</b> · AWS account</summary>

```mermaid
flowchart LR
  dev[Developer / Jenkins] --> runner
  subgraph runner["SCT runner"]
    test[SCT test - Python]
  end
  subgraph env["AWS account"]
    cp["EKS control plane"]
    db[("6 × Scylla pods")]
    ld["2 × loader pods"]
    mon["EC2 monitor"]
  end
  runner -->|"eksctl / EKS API"| env
  test -->|stress commands| ld
  ld -->|CQL| db
  test -.->|nemesis| db
  mon -.->|scrape| db
  test -->|results · errors| argus[(Argus)]
```

| Test type | What it does |
|---|---|
| [Longevity](../docs/longevity.md) | Long-running mixed load with chaos. The default shape most tests share. |
| [Scylla Manager](../docs/configuration_options/scylla-manager.md) | Backup, restore, repair and health checks driven by sctool. Backups go to the backend's object storage. |
| [Scylla Operator (k8s)](../docs/k8s-functional-test.md) | Functional tests of Scylla Operator. Scylla, loaders and supporting services all run as pods. |

</details>

<details><summary><b>GKE</b> · GCP project</summary>

```mermaid
flowchart LR
  dev[Developer / Jenkins] --> runner
  subgraph runner["SCT runner"]
    test[SCT test - Python]
  end
  subgraph env["GCP project"]
    cp["GKE control plane"]
    db[("6 × Scylla pods")]
    ld["2 × loader pods"]
    mon["GCE monitor VM"]
  end
  runner -->|"gcloud container"| env
  test -->|stress commands| ld
  ld -->|CQL| db
  test -.->|nemesis| db
  mon -.->|scrape| db
  test -->|results · errors| argus[(Argus)]
```

| Test type | What it does |
|---|---|
| [Longevity](../docs/longevity.md) | Long-running mixed load with chaos. The default shape most tests share. |
| [Scylla Manager](../docs/configuration_options/scylla-manager.md) | Backup, restore, repair and health checks driven by sctool. Backups go to the backend's object storage. |
| [Scylla Operator (k8s)](../docs/k8s-functional-test.md) | Functional tests of Scylla Operator. Scylla, loaders and supporting services all run as pods. |

</details>

<details><summary><b>kind</b> · kind cluster (docker)</summary>

```mermaid
flowchart LR
  dev[Developer / Jenkins] --> runner
  subgraph runner["Hydra on the host"]
    test[SCT test - Python]
  end
  subgraph env["kind cluster (docker)"]
    cp["kind control plane"]
    db[("6 × Scylla pods")]
    ld["2 × loader pods"]
    mon["monitoring containers"]
  end
  runner -->|"kind CLI"| env
  test -->|stress commands| ld
  ld -->|CQL| db
  test -.->|nemesis| db
  mon -.->|scrape| db
  test -->|results · errors| argus[(Argus)]
```

| Test type | What it does |
|---|---|
| [Longevity](../docs/longevity.md) | Long-running mixed load with chaos. The default shape most tests share. |
| [Scylla Manager](../docs/configuration_options/scylla-manager.md) | Backup, restore, repair and health checks driven by sctool. Backups go to the backend's object storage. |
| [Scylla Operator (k8s)](../docs/k8s-functional-test.md) | Functional tests of Scylla Operator. Scylla, loaders and supporting services all run as pods. |

</details>

<details><summary><b>ScyllaDB Cloud</b> · Your AWS account (xcloud_provider: aws)</summary>

```mermaid
flowchart LR
  dev[Developer / Jenkins] --> runner
  subgraph runner["SCT runner"]
    test[SCT test - Python]
  end
  subgraph env["Your AWS account (xcloud_provider: aws)"]
    db[("6 × managed ScyllaDB Cloud cluster")]
    ld["2 × EC2 loaders"]
    mon["EC2 monitor"]
  end
  runner -->|"Scylla Cloud REST API + EC2 API"| env
  test -->|stress commands| ld
  ld -->|CQL| db
  test -.->|nemesis| db
  mon -.->|scrape| db
  test -->|results · errors| argus[(Argus)]
```

| Test type | What it does |
|---|---|
| [Longevity](../docs/longevity.md) | Long-running mixed load with chaos. The default shape most tests share. |
| [Performance regression](../docs/performance/README.md) | Steady stress against a fixed cluster. HDR latency and throughput go to Argus, which compares with past runs and fails on regression. |
| [Vector search](../docs/configuration_options/vector-store.md) | Vector-store nodes attached to the Scylla cluster, serving ANN queries over HTTP. |

</details>

<details><summary><b>Bare metal</b> · Pre-provisioned hosts</summary>

```mermaid
flowchart LR
  dev[Developer / Jenkins] --> runner
  subgraph runner["SCT runner"]
    test[SCT test - Python]
  end
  subgraph env["Pre-provisioned hosts"]
    db[("6 × physical DB hosts")]
    ld["2 × physical loader hosts"]
    mon["physical monitor host"]
  end
  runner -->|"SSH only (no provisioning)"| env
  test -->|stress commands| ld
  ld -->|CQL| db
  test -.->|nemesis| db
  mon -.->|scrape| db
  test -->|results · errors| argus[(Argus)]
```

| Test type | What it does |
|---|---|
| [Longevity](../docs/longevity.md) | Long-running mixed load with chaos. The default shape most tests share. |

</details>

<details><summary><b>minicloud</b> · minicloud container (QEMU/KVM)</summary>

```mermaid
flowchart LR
  dev[Developer / Jenkins] --> runner
  subgraph runner["KVM host"]
    test[SCT test - Python]
  end
  subgraph env["minicloud container (QEMU/KVM)"]
    db[("6 × QEMU guest · AMI copy")]
    ld["2 × QEMU guest"]
    mon["QEMU guest"]
  end
  runner -->|"emulated EC2 API (localhost:5000)"| env
  test -->|stress commands| ld
  ld -->|CQL| db
  test -.->|nemesis| db
  mon -.->|scrape| db
  env --- s0["Amazon S3 (real)"]
  test -->|results · errors| argus[(Argus)]
```

| Test type | What it does |
|---|---|
| [Longevity](../docs/longevity.md) | Long-running mixed load with chaos. The default shape most tests share. |
| [Performance regression](../docs/performance/README.md) | Steady stress against a fixed cluster. HDR latency and throughput go to Argus, which compares with past runs and fails on regression. |
| [Rolling upgrade](../docs/rolling-upgrade.md) | Starts on a base version and upgrades one node at a time under load, optionally rolling some back. |
| [Artifacts](../docs/artifacts_test.md) | Checks a package, image or AMI on one node. No loaders, no monitor. |
| [Multi-DC longevity](../docs/longevity.md) | One cluster across regions. List values give per-DC counts; rack_aware_loader keeps load local. |
| [Alternator](../docs/configuration_options/alternator-dynamodb-api.md) | Same topology as longevity; the load uses the DynamoDB-compatible API instead of CQL. |
| [Kafka connectors](../docs/kafka.md) | Kafka runs as a docker-compose stack on the SCT runner. CDC source and sink connectors move data between Scylla and Kafka. |

</details>
