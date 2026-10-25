// vars/bestThroughputRequestPipeline.groovy
// Launcher of best-throughput runs (SCT-580). Thin wrapper: all logic lives in
// sdcm/utils/best_throughput_launcher.py, run by `sct.py create-best-throughput-job`.
//
// The request (this job's parameters) goes to Python as a JSON file, so user text such as
// test_config and extra_environment_variables never passes through shell quoting.
// Python validates it, creates the job of the requested hardware configuration in the
// best-throughput/ folder if it is missing, and starts it without waiting.

def call(Map pipelineParams = [:]) {
    pipeline {
        agent {
            label {
                label "aws-sct-builders-eu-west-1-v3-CI"
                retries 3
            }
        }

        environment {
            AWS_ACCESS_KEY_ID     = credentials('qa-aws-secret-key-id')
            AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
        }

        parameters {
            separator(name: 'JOB_IDENTITY', sectionHeader: 'Job identity: forms the job name, so it decides which runs share an Argus history')
            choice(name: 'backend', choices: ['aws'],
                   description: 'Cloud backend. Only AWS is supported for now.')
            string(name: 'instance_type_db', defaultValue: '',
                   description: 'Required. DB node instance type, e.g. i8g.4xlarge')
            string(name: 'n_db_nodes', defaultValue: '3',
                   description: 'Number of DB nodes')
            booleanParam(name: 'tablets', defaultValue: true,
                   description: 'Tablets keyspaces; unchecked runs vnodes (adds configurations/tablets_disabled.yaml)')
            string(name: 'job_name_suffix', defaultValue: '',
                   description: 'Optional. Gives an experiment its own job and history: <backend>-<instance>-<n>n-<tablets|vnodes>-<suffix>')

            separator(name: 'SETUP', sectionHeader: 'Setup')
            text(name: 'test_config', defaultValue: '',
                 description: '''JSON list of SCT config files. The first entry is the test-case; overlays follow and override it.
Empty: ["test-cases/performance/perf-regression-predefined-throughput-steps.yaml", "configurations/disable_kms.yaml", "configurations/disable_speculative_retry.yaml"]
Always added by the launcher: configurations/tablets_disabled.yaml (when tablets is unchecked) and the best-throughput overlay;
with the default test-case also its time-bound write commands and the dataset sized from the DB nodes' RAM and disk.

Test-cases (pick one, first in the list):
  test-cases/performance/perf-regression-predefined-throughput-steps.yaml                 cassandra-stress (default)
  test-cases/performance/perf-regression-predefined-throughput-steps-lwt-light.yaml       LWT, light
  test-cases/performance/perf-regression-predefined-throughput-steps-lwt-heavy.yaml       LWT, heavy
  test-cases/performance/perf-regression-predefined-throughput-steps-custom-d3-w1.yaml    latte, custom d3/w1
  test-cases/performance/latte-perf-regression-predefined-throughput-steps-tablets.yaml   latte, tablets
  test-cases/performance/latte-perf-regression-predefined-throughput-steps-vnodes.yaml    latte, vnodes

General:
  configurations/disable_kms.yaml                    (default)
  configurations/disable_speculative_retry.yaml      (default)
  configurations/c-s-driver-version-4.yaml           cassandra-stress Java driver 4

Stress tool (replaces the cassandra-stress commands):
  configurations/performance/rust-predefined-throughput-steps-cql-stress.yaml    cql-stress (Rust)
  configurations/performance/gocql-predefined-throughput-steps-stress.yaml       gocql stress

Data distribution, schema and storage:
  configurations/cs-uniform-distribution.yaml        uniform key population
  configurations/cs-gauss-distribution.yaml          gaussian key population
  configurations/cs-compaction-strategy-lcs.yaml     LCS compaction
  configurations/compression/zstd_dict_compression.yaml
  configurations/compression/lz4_dict_compression.yaml
  configurations/performance/enable_logstor.yaml     logstor; combine with one of:
  configurations/performance/logstor_uniform_queries.yaml
  configurations/performance/logstor_lsm_dual_60_40.yaml
  configurations/performance/logstor_hotspot_queries.yaml

Cloud specifics:
  configurations/gce/z4d-highmem-16-highlssd.yaml    z4d local-SSD layout (GCE)
  configurations/gce/z4d-highmem-16-standardlssd.yaml
  configurations/performance/xcloud/overrides.yaml   Scylla Cloud runs

Not for this job: step grids (cassandra_stress_gradual_load_steps*.yaml, *_reduced_steps_number.yaml,
latte_gradual_load_steps_enterprise.yaml, latte-perf-gradual-steps-custom-d3-w1.yaml) and per-step thresholds
(latency-decorator-error-thresholds-steps-*.yaml): the search replaces them.
Not needed: arm_instance_types/*.yaml (the architecture follows instance_type_db).''')
            text(name: 'extra_environment_variables', defaultValue: '',
                 description: '''KEY=VALUE lines passed to SCT, e.g. SCT_C_S_DRIVER_VERSION=4.
The launcher appends the variables of the parameters on this page; on a shared variable the parameter wins.
Setting SCT_INSTANCE_TYPE_DB or SCT_N_DB_NODES to a different value than above is rejected.''')
            string(name: 'region', defaultValue: '',
                   description: 'Empty: the backend default (us-east-1)')
            string(name: 'instance_type_loader', defaultValue: '',
                   description: "Empty: the test-case's (c7i.8xlarge)")
            string(name: 'n_loaders', defaultValue: '',
                   description: "Empty: the test-case's (4)")
            string(name: 'sub_tests', defaultValue: '["test_read_gradual_increase_load", "test_mixed_gradual_increase_load", "test_read_disk_only_gradual_increase_load", "test_write_gradual_increase_load"]',
                   description: 'JSON list of workloads to run, in parallel')
            string(name: 'provision_type', defaultValue: 'on_demand',
                   description: 'on_demand or spot')

            separator(name: 'SEARCH', sectionHeader: 'Search and result (empty: the best-throughput overlay default)')
            string(name: 'p99_limit_ms', defaultValue: '',
                   description: 'P99 limit a step must stay within. Empty: 10')
            string(name: 'step_duration', defaultValue: '',
                   description: 'Duration of every step, e.g. 30m. Empty: 30m')
            string(name: 'start_threads_per_shard', defaultValue: '',
                   description: 'Threads of the first step per DB shard, over all stress commands. Empty: 16')
            string(name: 'max_search_steps', defaultValue: '',
                   description: 'Step budget per workload. Empty: 8')
            string(name: 'search_precision_pct', defaultValue: '',
                   description: 'Stop when the passing and failing thread counts are this close, in %. Empty: 5')
            string(name: 'overload_factors', defaultValue: '',
                   description: 'Goodput under overload (SCYLLADB-3267): comma-separated factors of the best throughput to offer after the search, e.g. 1.3,2. Goodput under 0.9 of the best throughput fails the run. Empty: no overload steps')

            separator(name: 'PER_RUN', sectionHeader: 'Per run (not part of the job name; one job spans Scylla versions)')
            string(name: 'scylla_version', defaultValue: '',
                   description: 'Scylla version, e.g. 2026.2.0 or master:latest. This or one of the image parameters below is required.')
            string(name: 'scylla_ami_id', defaultValue: '', description: 'AMI ID for ScyllaDB')
            string(name: 'gce_image_db', defaultValue: '', description: 'GCE image for ScyllaDB')
            string(name: 'azure_image_db', defaultValue: '', description: 'Azure image for ScyllaDB')
            string(name: 'scylla_repo', defaultValue: '', description: 'Scylla repo')
            string(name: 'new_scylla_repo', defaultValue: '', description: 'New Scylla repo')
            string(name: 'update_db_packages', defaultValue: '', description: 'DB packages to update to')
            string(name: 'requested_by_user', defaultValue: '',
                   description: 'Actual user requesting the run, for automated requests (e.g. through Argus)')
            string(name: 'email_recipients', defaultValue: '',
                   description: "Email recipients of the run's report. Empty: the test-case's")

            booleanParam(name: 'dry_run', defaultValue: false,
                   description: 'Validate the request and print the job and its parameters, without creating or starting it')
        }

        options {
            timestamps()
            timeout(time: 1, unit: 'HOURS')
            buildDiscarder(logRotator(numToKeepStr: '100'))
        }

        stages {
            stage('Create and start the best-throughput job') {
                steps {
                    script {
                        def requestNames = [
                            'backend', 'instance_type_db', 'n_db_nodes', 'tablets', 'job_name_suffix',
                            'test_config', 'extra_environment_variables', 'region', 'instance_type_loader',
                            'n_loaders', 'sub_tests', 'provision_type',
                            'p99_limit_ms', 'step_duration', 'start_threads_per_shard', 'max_search_steps',
                            'search_precision_pct', 'overload_factors',
                            'scylla_version', 'scylla_ami_id', 'gce_image_db', 'azure_image_db', 'scylla_repo',
                            'new_scylla_repo', 'update_db_packages', 'requested_by_user', 'email_recipients',
                        ]
                        def request = [:]
                        requestNames.each { name -> request[name] = params[name] }
                        writeFile(file: 'best_throughput_request.json', text: groovy.json.JsonOutput.toJson(request))

                        currentBuild.description = "${params.backend}-${params.instance_type_db}-${params.n_db_nodes}n-${params.tablets ? 'tablets' : 'vnodes'}" +
                            (params.job_name_suffix?.trim() ? "-${params.job_name_suffix.trim()}" : '') +
                            (params.dry_run ? ' (dry run)' : '')

                        def cmd = "./docker/env/hydra.sh create-best-throughput-job --request-file best_throughput_request.json"
                        if (params.dry_run) {
                            cmd += " --dry-run"
                        }
                        sh(cmd)
                    }
                }
            }
        }

        post {
            always {
                cleanWs()
            }
        }
    }
}
