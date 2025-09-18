# SCT (Scylla Cluster Tests) Copilot Instructions

## Project Overview
SCT is a distributed testing framework for Scylla database that provisions multi-node clusters across cloud providers and executes complex test scenarios including load testing, chaos engineering (nemesis), and upgrade testing. The framework is designed around a "hydra" containerized execution model.

## Architecture Components

### Core Testing Framework
- **`sdcm/tester.py`**: Main `ClusterTester` base class - inherit from this for all test classes
- **`sct.py`**: CLI entrypoint with commands like `run-test`, `provision`, `clean-resources`
- **Hydra**: Docker-based execution environment (`docker/env/hydra.sh`) that wraps all SCT commands

### Test Structure Pattern
```python
class MyTest(ClusterTester):
    def test_my_scenario(self):
        # Test setup is handled by ClusterTester.setUp()
        # Access clusters via self.db_cluster, self.loaders, self.monitors
        pass
```

### Key Cluster Types
- **DB Clusters**: Scylla/Cassandra nodes (`self.db_cluster`, `self.db_clusters_multitenant`)
- **Loaders**: Execute stress tools like cassandra-stress (`self.loaders`, `self.loaders_multitenant`)
- **Monitors**: Prometheus/Grafana monitoring stack (`self.monitors`, `self.monitors_multitenant`)

### Backend Support Matrix
- **AWS**: Primary backend (`aws`) - full feature support
- **GCE**: Secondary backend (`gce`) - artifacts and upgrades
- **Docker**: Local development (`docker`) - limited nemesis support, see `docs/docker-backend-nemesis.md`
- **K8s**: Operator testing (`k8s-eks`, `k8s-gke`, `k8s-local-kind`)

## Essential Workflows

### Running Tests Locally
```bash
# Docker backend (recommended for development)
export SCT_SCYLLA_VERSION=5.2.1
hydra run-test longevity_test.LongevityTest.test_custom_time \
  --backend docker \
  --config test-cases/PR-provision-test-docker.yaml

# AWS backend (requires credentials)
export SCT_ENABLE_ARGUS=false  # Disable reporting during testing
hydra run-test longevity_test.LongevityTest.test_custom_time \
  --backend aws \
  --config test-cases/PR-provision-test.yaml \
  --config configurations/network_config/test_communication_public.yaml
```

### Configuration System
- **Test configs**: `test-cases/` directory contains YAML configurations
- **Shared configs**: `configurations/` for reusable settings (network, nemesis, etc.)
- **Multiple configs**: Chain with multiple `-c` flags - later configs override earlier ones
- **Environment variables**: Prefix with `SCT_` (e.g., `SCT_SCYLLA_VERSION=5.2.1`)

### Nemesis (Chaos Engineering)
- **Nemesis classes**: Defined in `sdcm/nemesis.py` - inherit from `Nemesis` base class
- **Generation tools**: Use `hydra create-nemesis-pipelines` to generate Jenkins pipelines
- **Docker limitations**: Some nemesis types skip on Docker backend (no hard reboot support)

## Project-Specific Patterns

### Test Configuration Inheritance
```yaml
# Base config
test_duration: 180
n_db_nodes: 6
nemesis_class_name: 'SisyphusMonkey'

# Override in environment or additional configs
stress_cmd: ["cassandra-stress write ..."]
```

### Cluster Access Patterns
```python
# Database connections
with self.db_cluster.cql_connection_patient(node) as session:
    session.execute("SELECT * FROM keyspace.table")

# Node operations
for node in self.db_cluster.nodes:
    node.run_nodetool("repair")
    node.remoter.run("systemctl restart scylla-server")
```

### Multi-Tenant Testing
- Access via `self.db_clusters_multitenant` for multiple clusters
- Common pattern for cross-DC and multi-region testing
- Supports parallel cluster operations with `ParallelObject`

### Logging and Results
- **Test logs**: `~/sct-results/latest/` contains all test artifacts
- **Test ID**: Generated UUID, access via `self.test_config.test_id()`
- **Event system**: Use `InfoEvent`, `ErrorEvent` for structured logging

## Development Guidelines

### Local Environment Setup
```bash
# Use uv + direnv for modern Python management (see docs/setup-uv-direnv.md)
# Or traditional setup:
sudo ./install-hydra.sh
hydra bash  # Enter containerized environment
```

### Test Development Pattern
1. Create test class inheriting from `ClusterTester`
2. Define test methods with `test_` prefix
3. Use existing config templates from `test-cases/`
4. Test locally with Docker backend first
5. Validate on AWS/GCE for final testing

### Jenkins Integration
- **Pipeline files**: `jenkins-pipelines/` organized by test type
- **Groovy templates**: Use `longevityPipeline()` function for standard tests
- **Triggers**: Automated via `sct_triggers/` for CI/CD

### Common Pitfalls
- **Network config**: Always specify network configuration for non-Docker backends
- **Resource cleanup**: Use `hydra clean-resources` to avoid orphaned resources
- **Argus reporting**: Disable with `SCT_ENABLE_ARGUS=false` for development
- **Schema synchronization**: Call `db_cluster.wait_for_schema_agreement()` after DDL operations

## Key Files to Reference
- **Core framework**: `sdcm/tester.py`, `sdcm/cluster.py`
- **Test examples**: `longevity_test.py`, `upgrade_test.py`
- **Config examples**: `test-cases/PR-provision-test*.yaml`
- **Docker specifics**: `docs/docker-backend-overview.md`
- **Nemesis reference**: `docs/docker-backend-nemesis.md`


## Additional Instructions for handling git commit

* When you are ready to commit your changes, make sure to follow the project's commit message guidelines.
* Use clear and descriptive commit messages that summarize the changes made.
* Squash related commits into a single commit to keep the history clean. do that on the end of each session of work before pushing.
* in the end of each session of work before pushing, make sure to fetch and rebase on top of the latest main branch to avoid merge conflicts
* in the end of each session of work before pushing, make sure to run:
  ```bash
    pre-commit run --all-files
  ```
