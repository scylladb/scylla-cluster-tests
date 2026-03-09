# Amazon EMR Integration for Scylla Spark Migrator Testing

## Problem Statement

There is currently no support in SCT for provisioning and managing Amazon EMR (Elastic MapReduce) clusters, which are required to test the [scylla-spark-migrator](https://github.com/scylladb/scylla-spark-migrator) — a Spark-based tool that migrates data between Cassandra/DynamoDB and ScyllaDB.

Testing the spark-migrator requires:

- A running Scylla cluster (source or target)
- A running Cassandra or DynamoDB instance (source or target)
- An Apache Spark environment to execute the migration job

Amazon EMR provides a turnkey managed Spark environment on AWS with:

- **Optimized Spark+YARN**: Fully tuned cluster ready in minutes with distributed networking handled automatically
- **EMRFS**: Highly optimized S3 connector, allowing S3 to function like HDFS
- **Spot Instance Integration**: Easy mixing of On-Demand (Master) and Spot (Core/Task) nodes for up to 80% cost savings
- **Version Management**: Bundled, tested Spark releases (though with some version lag vs. upstream)

The trade-off is an EMR management fee per second on top of raw EC2 costs.

Without EMR support, spark-migrator testing must be done manually or outside the SCT framework, preventing automated regression testing and CI/CD integration.

## Current State

SCT has no EMR-related code. The following analysis identifies existing patterns and integration points for adding EMR support.

### Resource Provisioning

- `sdcm/provision/provisioner.py` — Base `Provisioner` abstract class and `ProvisionerFactory` for registering backend-specific provisioners
- `sdcm/provision/aws/provisioner.py` — `AWSInstanceProvisioner` handles EC2 instance creation (on-demand, spot, mixed)
- `sdcm/provision/__init__.py` — Provisioner registration (currently registers Azure and GCE provisioners)
- `sdcm/cluster_aws.py:AWSCluster` — AWS cluster management extending `sdcm/cluster.py:BaseCluster`, handles node lifecycle (create, start, stop, destroy)
- `sdcm/cluster_aws.py:AWSNode` — Individual AWS node management with tagging, IP management, and monitoring

### Test Setup and Backend Routing

- `sdcm/tester.py:ClusterTester.init_resources()` (line ~2508) — Routes cluster creation based on `cluster_backend` value via if/elif chain:
  - `"aws"` / `"aws-siren"` → `get_cluster_aws()`
  - `"gce"` / `"gce-siren"` → `get_cluster_gce()`
  - `"k8s-eks"` → `get_cluster_k8s_eks()`
  - etc.
- `sdcm/tester.py:ClusterTester.get_cluster_aws()` (line ~1737) — Creates `ScyllaAWSCluster`, `LoaderSetAWS`, and `MonitorSetAWS`

### Configuration

- `sdcm/sct_config.py:SCTConfiguration.cluster_backend` (line ~474) — Backend selection field
- `sdcm/sct_config.py:SCTConfiguration.defaults_config_files` (line ~2164) — Maps backend names to default YAML configs (e.g., `"aws"` → `defaults/aws_config.yaml`, `"k8s-eks"` → `[defaults/aws_config.yaml, defaults/k8s_eks_config.yaml]`)
- `defaults/aws_config.yaml` — AWS-specific defaults (instance types, AMIs, regions, spot config)
- `defaults/k8s_eks_config.yaml` — Example of layered config on top of AWS defaults

### Resource Cleanup

- `sdcm/utils/resources_cleanup.py:clean_cloud_resources()` (line ~73) — Main cleanup orchestrator; conditionally calls backend-specific cleanup functions based on `cluster_backend`
- `sdcm/utils/resources_cleanup.py:clean_instances_aws()` (line ~179) — Terminates EC2 instances by tag filtering (`TestId`, `RunByUser`)
- `sdcm/utils/resources_cleanup.py:clean_clusters_eks()` (line ~511) — EKS-specific cleanup: discovers EKS clusters by tags and destroys them in parallel using `ParallelObject`

### Email Reporting

- `sdcm/send_email.py:BaseEmailReporter` (line ~126) — Base class with common fields, Jinja2 template rendering, and SMTP delivery
- `sdcm/send_email.py:build_reporter()` (line ~589) — Maps test class names to reporter subclasses (e.g., `"Longevity"` → `LongevityEmailReporter`)
- `sdcm/report_templates/` — Jinja2 HTML templates for each reporter type

### Tagging and Resource Discovery

- All AWS resources are tagged with `TestId` and `RunByUser` for cleanup discovery
- `sdcm/utils/aws_utils.py:tags_as_ec2_tags()` — Converts tag dictionaries to EC2 API format
- Cleanup functions use `list_instances_aws()` and similar utilities to discover resources by tags

### What's Missing

- No EMR cluster provisioner or cluster class
- No EMR configuration parameters in `sdcm/sct_config.py`
- No EMR cleanup function in `resources_cleanup.py`
- No spark-migrator test class or test cases
- No EMR-specific email reporter or template
- No default configuration for EMR in `defaults/`

## Goals

1. **Provision EMR clusters programmatically** within the SCT framework, following existing patterns for AWS resource management
2. **Support Spot Instance integration** for EMR Core/Task nodes to minimize cost (On-Demand for Master, Spot for Core/Task)
3. **Submit and monitor spark-migrator jobs** on provisioned EMR clusters with configurable Spark parameters
4. **Clean up EMR resources automatically** using the same tag-based approach as existing AWS resources (`TestId`, `RunByUser`)
5. **Integrate with SCT email reporting** so spark-migrator test results appear in standard SCT reports
6. **Provide a reusable test base class** for spark-migrator tests that orchestrates Scylla cluster + EMR cluster + migration job lifecycle
7. **Support configurable EMR release versions** (e.g., `emr-7.x`) and Spark versions to test across multiple environments

## Implementation Phases

### Phase 1: EMR Configuration Parameters

**Importance**: Critical
**Description**: Add EMR-specific configuration fields to `sdcm/sct_config.py` and create default configuration files. This is the foundation for all subsequent phases.

**Deliverables**:
- New configuration fields in `sdcm/sct_config.py`:
  - `emr_release_label` — EMR release version (e.g., `"emr-7.8.0"`)
  - `emr_instance_type_master` — Instance type for EMR master node (e.g., `"m5.xlarge"`)
  - `emr_instance_type_core` — Instance type for EMR core nodes
  - `emr_instance_count_core` — Number of EMR core nodes (default: `2`)
  - `emr_instance_type_task` — Instance type for EMR task nodes (optional Spot)
  - `emr_instance_count_task` — Number of EMR task nodes (default: `0`)
  - `emr_spot_bid_percentage` — Max Spot price as percentage of On-Demand (default: `100`)
  - `emr_applications` — List of EMR applications to install (default: `["Spark"]`)
  - `emr_spark_migrator_jar_path` — S3 path or local path to the spark-migrator JAR
  - `emr_log_uri` — S3 URI for EMR cluster logs
  - `emr_keep_alive` — Whether EMR cluster stays alive after job completion (default: `true` for reuse during testing)
- EMR IAM roles (`EMR_DefaultRole`, `EMR_EC2_DefaultRole`) are **not configurable** — they are standard AWS-managed roles created once per account. `AwsRegion` (in `sdcm/utils/aws_region.py`) will obtain them by name, following the existing pattern for VPCs, subnets, security groups, and keypairs.
- New default config: `defaults/aws_emr_config.yaml`
- Backend mapping entry for `"aws"` backend (EMR is an add-on resource, not a separate backend)

**Definition of Done**:
- [ ] Configuration fields added with descriptions and types
- [ ] Default YAML configuration created
- [ ] Configuration loads and validates correctly
- [ ] Unit tests verify config parsing for new EMR fields
- [ ] `uv run sct.py pre-commit` passes

---

### Phase 2: EMR Cluster Provisioner

**Importance**: Critical
**Description**: Create the EMR cluster provisioning module using the AWS `boto3` EMR API. Follow the pattern of `sdcm/provision/aws/` for EC2 resources.

**Dependencies**: Phase 1

**Deliverables**:
- New file: `sdcm/provision/aws/emr_provisioner.py` — EMR cluster lifecycle management:
  - `create_emr_cluster()` — Creates EMR cluster with configured instance groups (Master on-demand, Core/Task on Spot), tags with `TestId`/`RunByUser`, installs configured applications
  - `wait_for_emr_cluster_ready()` — Polls cluster status until `WAITING` state
  - `terminate_emr_cluster()` — Terminates EMR cluster by cluster ID
  - `get_emr_cluster_status()` — Returns current cluster state
  - `get_emr_master_dns()` — Returns master node public DNS for Spark UI/SSH access
- `AwsRegion` extensions in `sdcm/utils/aws_region.py`:
  - `emr_service_role` property — Obtains `EMR_DefaultRole` IAM role by name
  - `emr_ec2_instance_profile` property — Obtains `EMR_EC2_DefaultRole` instance profile by name
  - `ensure_emr_roles()` — Creates default EMR roles if they don't exist (equivalent to `aws emr create-default-roles`)
- Tag propagation: All EMR instances tagged with `TestId`, `RunByUser`, `NodeType: "emr"` via EMR API `Tags` parameter
- Instance fleet configuration: Master (On-Demand), Core (configurable), Task (Spot with bid percentage)

**Definition of Done**:
- [ ] EMR cluster can be created, waited on, and terminated
- [ ] All EMR instances are tagged for cleanup discovery
- [ ] Spot/On-Demand instance configuration works correctly
- [ ] Unit tests using `moto` (via `ThreadedMotoServer`) verify provisioning logic against mocked EMR APIs
- [ ] Integration test with `moto` verifies full EMR cluster lifecycle (mark with `@pytest.mark.integration`)

---

### Phase 3: EMR Resource Cleanup

**Importance**: Critical
**Description**: Add EMR cluster cleanup to the existing resource cleanup pipeline to prevent orphaned EMR clusters from accumulating costs.

**Dependencies**: Phase 2

**Deliverables**:
- New function in `sdcm/utils/resources_cleanup.py`:
  - `clean_emr_clusters(tags_dict, regions=None, dry_run=False)` — Discovers EMR clusters by tag, terminates them
  - Pattern follows `clean_clusters_eks()`: list by tags → parallel terminate → log results
- Integration into `clean_cloud_resources()`: Call `clean_emr_clusters()` when `cluster_backend` includes `"aws"`
- Discovery function: `list_emr_clusters(tags_dict, region_name)` in `sdcm/utils/common.py` or similar utility module

**Definition of Done**:
- [ ] `clean_emr_clusters()` discovers and terminates EMR clusters by tags
- [ ] `clean_cloud_resources()` calls EMR cleanup for AWS backends
- [ ] `dry_run=True` logs what would be deleted without terminating
- [ ] Unit tests using `moto` verify cleanup logic against mocked EMR APIs
- [ ] No orphaned EMR clusters after test teardown

---

### Phase 4: Spark Migrator Job Submission

**Importance**: Critical
**Description**: Implement Spark job submission to EMR clusters for running the scylla-spark-migrator, including job monitoring and result collection.

**Dependencies**: Phase 2

**Deliverables**:
- New file: `sdcm/spark_migrator.py` (or `sdcm/utils/spark_migrator.py`) — Job submission and monitoring:
  - `submit_spark_migrator_job(emr_cluster_id, migrator_config)` — Submits spark-migrator JAR as an EMR Step
  - `wait_for_step_completion(emr_cluster_id, step_id)` — Polls step status until completion/failure
  - `get_step_logs(emr_cluster_id, step_id)` — Retrieves step stdout/stderr from S3
  - `build_spark_submit_args(migrator_config)` — Constructs `spark-submit` arguments for the migrator JAR
- Migrator configuration model (Pydantic):
  - Source/target connection details (Scylla, Cassandra, DynamoDB endpoints)
  - Keyspace/table selection
  - Spark configuration overrides (executor memory, cores, parallelism)
  - Migration mode (full table, incremental, specific token ranges)

**Definition of Done**:
- [ ] Spark migrator job can be submitted and monitored on an EMR cluster
- [ ] Job logs are retrieved and stored in the SCT log directory
- [ ] Job success/failure is correctly detected and reported
- [ ] Unit tests using `moto` with mocked EMR Step API verify submission logic
- [ ] Configuration model validates inputs correctly

---

### Phase 5: Test Base Class and Test Case

**Importance**: Critical
**Description**: Create a reusable test base class that orchestrates the full spark-migrator test lifecycle: provision Scylla + EMR → load data → run migration → validate → clean up.

**Dependencies**: Phases 1–4

**Deliverables**:
- New file: `spark_migrator_test.py` (top-level test module) — Test class:
  - `SparkMigratorTest(ClusterTester)` — Extends `sdcm/tester.py:ClusterTester`
  - `setUp()`: Creates Scylla cluster via existing AWS backend + EMR cluster via Phase 2 provisioner
  - `test_full_migration()`: Loads data into source → submits migration job → validates data in target
  - `tearDown()`: Destroys EMR cluster, Scylla cleanup handled by parent
- New test case YAML: `test-cases/spark-migrator/spark-migrator-basic.yaml` — Configuration for a basic migration test
- EMR cluster initialization integrated into `sdcm/tester.py:init_resources()` (conditional on EMR config being present — does not add a new backend, but adds EMR as an auxiliary resource alongside the Scylla cluster)

**Adaptation Notes**: The EMR cluster is an auxiliary resource alongside the Scylla cluster, not a replacement. The `cluster_backend` remains `"aws"` and EMR is provisioned when `emr_release_label` is configured. This follows the pattern of how Kafka clusters are handled as add-on resources (see `self.get_cluster_kafka()` in `init_resources()`).

**Definition of Done**:
- [ ] Test class provisions Scylla cluster + EMR cluster
- [ ] Migration job executes successfully and data is validated
- [ ] Both clusters are cleaned up in tearDown
- [ ] Test case YAML works with `uv run sct.py run-test spark_migrator_test.SparkMigratorTest.test_full_migration --backend aws`
- [ ] EMR cluster tagged correctly for cleanup discovery

---

### Phase 6: Email Reporting for Spark Migrator Tests

**Importance**: Important
**Description**: Add email reporting support so spark-migrator test results are included in standard SCT email reports.

**Dependencies**: Phase 5

**Deliverables**:
- New reporter class in `sdcm/send_email.py`:
  - `SparkMigratorEmailReporter(BaseEmailReporter)` — Adds fields for migration-specific metrics (rows migrated, duration, throughput, EMR cluster details)
- New email template: `sdcm/report_templates/results_spark_migrator.html`
- Registration in `build_reporter()` function: `"SparkMigrator"` → `SparkMigratorEmailReporter`
- Reporter fields:
  - `emr_cluster_id` — EMR cluster identifier
  - `emr_release_label` — EMR version used
  - `migration_duration` — Time to complete migration
  - `rows_migrated` — Number of rows transferred
  - `migration_throughput` — Rows/second or MB/second

**Definition of Done**:
- [ ] Email reporter renders migration test results correctly
- [ ] `build_reporter()` maps spark-migrator test class to the new reporter
- [ ] Template includes EMR cluster info and migration metrics
- [ ] Unit test verifies reporter data building and template rendering

---

### Phase 7: Post-Behavior Configuration and Documentation

**Importance**: Important
**Description**: Add post-behavior configuration for EMR clusters (keep/destroy after test) and document the full EMR integration.

**Dependencies**: Phase 5

**Deliverables**:
- New configuration field: `post_behavior_emr_cluster` in `sdcm/sct_config.py` (default: `"destroy"`)
- Default value in `defaults/test_default.yaml`
- Teardown logic in test base class respects post-behavior setting
- Documentation:
  - Update `docs/configuration_options.md` (auto-generated by pre-commit from `sct_config.py`)
  - New guide: `docs/spark-migrator-testing.md` — Setup, configuration, and running spark-migrator tests
  - Update `AGENTS.md` and project README if needed

**Definition of Done**:
- [ ] `post_behavior_emr_cluster` controls whether EMR is kept or destroyed after test
- [ ] Documentation covers setup prerequisites (IAM roles, S3 buckets, VPC configuration)
- [ ] Configuration regenerated via `uv run sct.py pre-commit`
- [ ] Guide includes example test execution commands

---

### Phase 8: Jenkins Pipeline Integration

**Importance**: Nice-to-have
**Description**: Create Jenkins pipeline definitions for running spark-migrator tests in CI/CD.

**Dependencies**: Phase 5

**Deliverables**:
- New Jenkins pipeline: `jenkins-pipelines/oss/spark-migrator/spark-migrator-basic.jenkinsfile`
- Pipeline configuration: instance types, regions, EMR parameters
- Scheduled or on-demand trigger configuration

**Definition of Done**:
- [ ] Jenkins pipeline runs spark-migrator test end-to-end
- [ ] Pipeline cleans up all resources including EMR clusters
- [ ] Pipeline sends email reports on completion

## Testing Requirements

### Unit Tests

- **Configuration parsing**: Verify EMR config fields parse correctly (valid/invalid inputs, defaults, type validation)
  - Location: `unit_tests/test_config.py` (extend existing config tests)
- **EMR provisioner**: Use `moto` (already a project dependency — `moto[server]==5.1.1` in `pyproject.toml`) to mock EMR APIs (`run_job_flow`, `describe_cluster`, `terminate_job_flows`). Follow the existing `ThreadedMotoServer` pattern in `unit_tests/test_aws_services.py`.
  - Location: `unit_tests/provision/test_emr_provisioner.py`
- **EMR cleanup**: Use `moto` to mock EMR API responses (`list_clusters`, `describe_cluster`, `terminate_job_flows`) for tag-based discovery and termination
  - Location: `unit_tests/test_clean_cloud_resources_func.py` (extend existing cleanup tests)
- **Spark job submission**: Use `moto` to mock EMR Steps API (`add_job_flow_steps`, `describe_step`, `list_steps`) for job submission and monitoring
  - Location: `unit_tests/test_spark_migrator.py`
- **Email reporter**: Verify data building and template rendering for migration-specific fields
  - Location: `unit_tests/test_send_email.py` (extend existing email tests)
- Run with: `uv run sct.py unit-tests -t <test_file>`

### Integration Tests

- **EMR lifecycle**: Use `moto` with `ThreadedMotoServer` (following the pattern in `unit_tests/test_aws_services.py`) to test the full EMR provisioner lifecycle — create cluster, wait for ready state, submit steps, and terminate. Moto supports the needed EMR APIs: `run_job_flow`, `describe_cluster`, `add_job_flow_steps`, `list_clusters`, `terminate_job_flows`.
  - Mark with: `@pytest.mark.integration`
  - No real AWS credentials required — moto provides a fully mocked AWS environment
- **End-to-end migration**: Run spark-migrator on a Docker or small AWS Scylla cluster
  - Run with: `uv run sct.py integration-tests`

### Manual Testing

- **Full-scale migration test**: Run on AWS with production-like data volumes
  - Command: `uv run sct.py run-test spark_migrator_test.SparkMigratorTest.test_full_migration --backend aws --config test-cases/spark-migrator/spark-migrator-basic.yaml`
- **Spot Instance behavior**: Verify Spot interruption handling on EMR Task nodes
- **Multi-region**: Test EMR cluster in different AWS regions
- **Cleanup verification**: Confirm no orphaned EMR clusters after test abort/failure
- **Email report**: Verify email report renders correctly with migration metrics

## Success Criteria

All Definition of Done items across phases are met. Additionally:

1. A spark-migrator test can be executed end-to-end using standard SCT commands (`uv run sct.py run-test`)
2. EMR clusters are cleaned up reliably — no orphaned clusters after 24 hours
3. Configuration documented in `docs/configuration_options.md` (auto-generated)
4. No regressions in existing unit tests
5. Cost-optimized: Spot Instances used for Core/Task nodes by default

## Risk Mitigation

### Risk: EMR Cluster Creation Failures

**Likelihood**: Medium
**Impact**: Test cannot start; EMR cluster remains in TERMINATED_WITH_ERRORS state with potential partial resource creation
**Mitigation**:
- Implement retry logic with exponential backoff for transient EMR API errors
- Ensure cleanup function handles clusters in any state (STARTING, BOOTSTRAPPING, TERMINATED_WITH_ERRORS)
- Log detailed EMR cluster creation failure reasons from `StateChangeReason`

### Risk: Spot Instance Interruptions During Migration

**Likelihood**: Medium (depends on instance type and region)
**Impact**: Migration job fails mid-execution, requiring restart
**Mitigation**:
- Use On-Demand for Master node (always)
- Configure EMR managed scaling to replace interrupted Spot instances
- Use checkpointing in spark-migrator if supported
- Document which instance types have lowest interruption rates per region

### Risk: EMR Version Lag vs. Spark Requirements

**Likelihood**: Low
**Impact**: Required Spark features not available in latest EMR release
**Mitigation**:
- Make `emr_release_label` configurable per test case
- Document supported EMR release → Spark version mapping
- Support custom bootstrap actions to install specific Spark versions if needed

### Risk: IAM Permission Complexity

**Likelihood**: High
**Impact**: EMR cluster creation fails due to missing IAM roles or policies
**Mitigation**:
- EMR IAM roles (`EMR_DefaultRole`, `EMR_EC2_DefaultRole`) are obtained by `AwsRegion` by their well-known names — not user-configurable
- `AwsRegion` will provide an `ensure_emr_roles()` method that checks for role existence and creates them if missing (via `aws emr create-default-roles` equivalent using boto3 IAM API), following the pattern of `create_sct_key_pair()` / `create_sct_vpc()`
- Add pre-flight check that validates IAM roles exist before attempting EMR creation
- Document IAM prerequisites in `docs/`

### Risk: Cost Overrun from Orphaned EMR Clusters

**Likelihood**: Medium
**Impact**: Significant AWS charges from forgotten running EMR clusters
**Mitigation**:
- Tag all EMR resources with `TestId` and `RunByUser` for cleanup discovery
- EMR cleanup integrated into `clean_cloud_resources()` for automatic cleanup
- Default `post_behavior_emr_cluster: "destroy"` ensures cleanup on normal teardown
- Consider adding EMR auto-termination idle timeout (EMR native feature) as a safety net

### Risk: Network Connectivity Between EMR and Scylla Clusters

**Likelihood**: Medium
**Impact**: Spark jobs cannot connect to Scylla nodes, migration fails
**Mitigation**:
- Ensure EMR cluster is provisioned in same VPC/subnet as Scylla cluster
- Configure security groups to allow traffic between EMR nodes and Scylla nodes on CQL port (9042)
- Document VPC and security group requirements

## Open Questions

- **Resolved**: EMR requires two IAM roles per AWS account (created once via `aws emr create-default-roles` or `AwsRegion.ensure_emr_roles()`):
  - `EMR_DefaultRole` — service role with managed policy `AmazonEMRServicePolicy_v2`
  - `EMR_EC2_DefaultRole` — EC2 instance profile with managed policy `AmazonEMREC2InstancePolicy_v2`
  - See [AWS docs: Configure IAM service roles for Amazon EMR](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-roles.html)
- **Resolved — S3 bucket structure**: Create a dedicated S3 bucket (e.g., `sct-emr-spark-migrator-{region}`) with the following layout:
  - `jars/{migrator-version}/` — spark-migrator JAR files, organized per release version of scylla-spark-migrator
  - `logs/{test-id}/` — EMR cluster logs, partitioned per SCT test run ID for easy correlation and cleanup
  - Bucket lifecycle policy: auto-expire log prefixes after 30 days; JAR prefixes are retained indefinitely
- **Nice-to-have**: EMR cluster reuse across multiple test cases (similar to `SCT_REUSE_CLUSTER` for Scylla clusters). Reusing an EMR cluster independently of the Scylla test cluster is even lower priority.
