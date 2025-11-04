# AGENTS.md

This file provides guidance to LLM Agents when working with code in this repository.

## Repository Overview

Scylla Cluster Tests (SCT) is a comprehensive test framework for ScyllaDB database, designed to test high-load scenarios on physical/virtual servers. The framework automatically provisions test infrastructure including Scylla clusters, loader machines, and monitoring servers.

## Development Environment Setup

### Local Development with uv (Recommended for direct Python work)
```bash
uv python install
uv venv
export UV_PROJECT_ENVIRONMENT=.venv
uv sync
```

### Using Hydra (Docker/Podman container environment)
```bash
# Install hydra
sudo ./install-hydra.sh

# Enter containerized environment
hydra bash
```

## Common Development Commands

#### Run unit tests
```bash
# All unit tests
uv run sct.py unit-tests

# Specific unit test
uv run sct.py unit-tests -t test_config.py
```

#### Run integration tests
```bash
uv run sct.py integration-tests
```

### Code Quality and Linting

```bash
# Run pre-commit checks (includes autopep8, ruff, and other linters)
uv run sct.py pre-commit

# Run specific linters
ruff check --fix --preview .
autopep8 -i -j 2 <file>
```

### Running SCT

```bash
export SCT_SCYLLA_VERSION=2025.3.0
export SCT_USE_MGMT=false
uv run sct.py run-test longevity_test.LongevityTest.test_custom_time --backend docker --config test-cases/PR-provision-test.yaml
# or with hydra (without need to install python dependencies):
./docker/env/hydra.sh run-test longevity_test.LongevityTest.test_custom_time --backend docker --config test-cases/PR-provision-test.yaml
```

#### Cluster Reuse (for faster development iterations)
Before running SCT run below command (only when previously left cluster alive):
```bash
# Reuse existing cluster
export SCT_REUSE_CLUSTER=$(cat ~/sct-results/latest/test_id)
```

## High-Level Architecture

### Test Organization

**Test Types:**
- `*_test.py` - Main test modules (longevity, performance, upgrade, artifacts, etc.)
- `functional_tests/` - Kubernetes operator and other functional tests
- `unit_tests/` - Unit and integration tests for SCT components

**Key Test Categories:**
1. **Longevity Tests** - Long-running stability tests with various nemesis operations
2. **Performance Tests** - Throughput and latency regression tests
3. **Upgrade Tests** - Rolling upgrade and version migration tests
4. **Artifacts Tests** - Package and image deployment verification
5. **Manager Tests** - Scylla Manager operation tests
6. **K8S Functional Tests** - Kubernetes operator tests

### Core Components

**Main Framework (`sdcm/`):**

The `sdcm/` directory is the heart of the SCT framework. Here's a detailed breakdown of its structure:

#### Core Files
- `cluster.py` - Base classes for cluster management, node operations, and connectivity
- `tester.py` - Base test class used by all tests, handles test setup/teardown and reporting
- `nemesis.py` - Core chaos engineering framework with disruptors for testing resilience
- `nemesis_registry.py` - Registry of all chaos operations available for testing
- `sct_config.py` - Configuration management and parameter handling
- `log.py` - Logging setup and utilities
- `db_stats.py` - Database metrics collection using Prometheus
- `loader.py` - Base classes for load generators
- `prometheus.py` - Prometheus integration for metrics

#### Specialized Clusters
- `cluster_aws.py` - AWS-specific cluster implementations
- `cluster_gce.py` - Google Cloud specific implementations
- `cluster_azure.py` - Azure-specific cluster implementations
- `cluster_docker.py` - Local Docker cluster implementation
- `cluster_baremetal.py` - Physical hardware cluster implementation
- `cluster_k8s/` - Kubernetes cluster implementations
  - `eks.py` - AWS EKS implementation
  - `gke.py` - Google Kubernetes Engine implementation
  - `mini_k8s.py` - Local Kubernetes implementation

#### Stress Test Tools
- `stress/` - Stress tool wrappers
  - `base.py` - Common base for stress tools
  - Various stress tool implementations (cassandra-stress, scylla-bench, etc.)
- `cassandra_harry_thread.py` - CassandraHarry consistency verification tool
- `cql_stress_cassandra_stress_thread.py` - CQL stress execution
- `stress_thread.py` - Base thread for running stress tools
- `gemini_thread.py` - Gemini consistency verification tool
- `scylla_bench_thread.py` - ScyllaBench stress tool wrapper
- `ycsb_thread.py` - YCSB benchmark wrapper
- `ndbench_thread.py` - NdBench benchmark wrapper

#### Remote Execution
- `remote/` - Command execution framework
  - `base.py` - Base command execution classes
  - `remote_cmd_runner.py` - SSH-based command execution
  - `docker_cmd_runner.py` - Docker-based command execution
  - `kubernetes_cmd_runner.py` - Kubernetes-based command execution
  - `remote_file.py` - Remote file operations

#### Monitoring & Reporting
- `monitorstack/` - Monitoring infrastructure setup and management
- `logcollector.py` - Log collection utilities
- `reporting/` - Test result reporting
  - `elastic_reporter.py` - Elasticsearch-based reporting
  - `tooling_reporter.py` - Tools and libraries reporting
- `parallel_timeline_report/` - Timeline visualization of events

#### Scylla Manager Integration
- `mgmt/` - Scylla Manager integration
  - `cli.py` - Manager CLI interface
  - `operations.py` - Manager operations (backup, repair, etc.)
  - `common.py` - Common manager utilities

#### Provisioning
- `provision/` - Infrastructure provisioning
  - `aws/` - AWS-specific provisioning
  - `azure/` - Azure-specific provisioning
  - `common/` - Common provisioning utilities
  - `scylla_yaml/` - Scylla configuration generation
  - `network_configuration.py` - Network setup
  - `security.py` - Security configuration

#### REST APIs and Clients
- `rest/` - REST API clients
  - `rest_client.py` - Base REST client
  - `storage_service_client.py` - Scylla storage service client
  - `compaction_manager_client.py` - Compaction manager API
  - `raft_api.py` - RAFT consensus protocol API

#### Results Analysis
- `results_analyze/` - Test result analysis
  - `base.py` - Base analysis framework
  - `metrics.py` - Metrics processing

#### Event System
- `sct_events/` - Event handling system
  - Framework for event generation, filtering, and processing
  - Events can be used for test flow control, reporting, and validation

#### Utility Modules
- `utils/` - Extensive utility library
  - `aws_utils.py`, `gce_utils.py`, `azure_utils.py` - Cloud provider utilities
  - `common.py` - Common utilities used throughout the framework
  - `docker_utils.py` - Docker-specific utilities
  - `version_utils.py` - Version comparison and management
  - `nemesis_utils/` - Utilities for nemesis operations
  - `replication_strategy_utils.py` - Replication strategy handling
  - `sstable/` - SSTable utilities
  - `k8s/` - Kubernetes utilities
  - `ldap.py` - LDAP authentication utilities
  - And many more specialized utilities

**Configuration System:**
- `test-cases/` - Test configuration YAML files
- `configurations/` - Reusable configuration fragments
- `defaults/` - Default configuration values
- Configuration precedence: CLI args > Environment vars > Config files > Defaults

**Backends Supported:**
- `aws` - Amazon Web Services (primary backend)
- `gce` - Google Cloud Platform
- `azure` - Microsoft Azure
- `docker` - Local Docker development
- `k8s-eks`, `k8s-gke` - Kubernetes on cloud providers
- `k8s-local-kind` - Local Kubernetes with kind
- `baremetal` - Pre-existing clusters

### Nemesis Operations

Nemesis are chaos operations that test database resilience. Common types:
- Node operations (stop/start, reboot, terminate, decommission)
- Network disruptions (block, delay, partition)
- Disk operations (fill disk, corrupt data)
- Schema changes (add/drop columns, modify tables)
- Load operations (memory stress, CPU stress)

### Monitoring and Reporting

**Monitoring Stack:**
- Prometheus + Grafana for metrics
- Elasticsearch for logs
- Custom SCT monitoring dashboards

**Test Results:**
- Results stored in `~/sct-results/`
- Argus integration for test tracking
- Email reports for performance tests
- Parallel timeline reports for debugging

## Testing Best Practices

### Test Configuration
- Start with existing test cases in `test-cases/` as templates
- Use configuration fragments from `configurations/` for common settings
- Network configs are in `configurations/network_config/`

### Development Workflow
1. Create feature branch from master
2. Run unit tests locally: `uv run sct.py unit-tests`
3. Test with docker backend first: `--backend docker`
4. Use cluster reuse for faster iteration
5. Run pre-commit before pushing: `uv run sct.py pre-commit`

### Debugging SCT Tests
- Check logs in `~/sct-results/latest/`

## Environment Variables

Critical environment variables:
- `SCT_CLUSTER_BACKEND` - Backend to use (aws, gce, azure, docker, etc.)
- `SCT_CONFIG_FILES` - Configuration files to use
- `SCT_SCYLLA_VERSION` - Scylla version to test
- `SCT_REUSE_CLUSTER` - Test ID to reuse existing cluster
- `SCT_ENABLE_ARGUS` - Enable/disable Argus reporting
- `SCT_IP_SSH_CONNECTIONS` - Use 'public' for local development
- `AWS_PROFILE` - AWS profile for authentication (if using OKTA)

## Common Issues and Solutions

### AWS/Cloud Authentication
- Use OKTA for AWS access (preferred)
- Or configure AWS CLI: `aws configure`
- For GCE: Set up gcloud CLI
- For Azure: Configure az CLI

### Network Configuration
- For local development with cloud backend (aws, gce, azure) use `configurations/network_config/test_communication_public.yaml` and `export SCT_IP_SSH_CONNECTIONS=public`

### Docker/Podman Issues
- Ensure user is in docker group: `sudo usermod -aG docker $USER`
- For podman, configure registries in `~/.config/containers/registries.conf`
- Some operations requiring sudo won't work in rootless podman

## Jenkins Pipeline Structure

Jenkins pipelines are in `jenkins-pipelines/` organized by:
- `oss/` - Scylla tests
- `operator/` - Kubernetes operator tests
- `performance/` - Performance regression tests
- `manager/` - Scylla Manager tests

Pipeline utilities are in `vars/` directory.
