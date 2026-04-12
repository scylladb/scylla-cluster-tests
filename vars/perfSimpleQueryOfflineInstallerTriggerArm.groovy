def call(Map pipelineParams) {
    def builder = getJenkinsLabels("aws", "eu-west-1")
    pipeline {
        agent {
            label {
                   label builder.label
            }
        }
        environment {
            AWS_ACCESS_KEY_ID     = credentials('qa-aws-secret-key-id')
            AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
            SCT_BILLING_PROJECT = "${params.billing_project}"
        }

        parameters {
            string(name: 'unified_package', defaultValue: '', description: 'URL to the unified package of scylla version to install scylla (e.g. from PGO build)')
            booleanParam(name: 'nonroot_offline_install', defaultValue: true, description: 'Install Scylla without required root privilege')
            string(defaultValue: '',
                   description: 'Actual user requesting job start, for automated job builds (e.g. through Argus)',
                   name: 'requested_by_user')
            choice(choices: getBillingProjectChoices(),
                   description: 'Billing project for the test run (dynamically fetched from finops repository)',
                   name: 'billing_project')
        }

        stages {
            stage('Trigger perf-simple-query ARM Jobs') {
                steps {
                    script {
                        def unified_package = params.unified_package?.trim()
                        if (!unified_package) {
                            error "unified_package parameter is required. Provide a URL to the unified (relocatable) Scylla package."
                        }

                        def testMatrix = [
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-simple-query-weekly-microbenchmark_arm64',
                                region: 'us-east-1',
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-simple-query-weekly-microbenchmark_arm64-write',
                                region: 'us-east-1',
                            ],
                        ]
                        println("testMatrix: $testMatrix")
                        for (def entry in testMatrix) {
                            catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                                println("Building job: ${entry.job_name} with unified_package: ${unified_package}, nonroot_offline_install: ${params.nonroot_offline_install}")
                                build job: entry.job_name, wait: false, parameters: [
                                    string(name: 'unified_package', value: unified_package),
                                    booleanParam(name: 'nonroot_offline_install', value: params.nonroot_offline_install),
                                    string(name: 'provision_type', value: 'on_demand'),
                                    string(name: 'region', value: entry.region),
                                    string(name: 'requested_by_user', value: params.requested_by_user),
                                    string(name: 'billing_project', value: params.billing_project),
                                ]
                            }
                        }
                    }
                }
            }
        }
    }
}
