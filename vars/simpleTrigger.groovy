def call(List jobs) {
    pipeline {
        agent none
        parameters {
            string(name: "scylla_version", defaultValue: "", description: "Version of ScyllaDB")
            string(name: "scylla_ami_id", defaultValue: "", description: "AMI ID for ScyllaDB")
            string(name: "region",
                   defaultValue: "'eu-west-1'",
                   description: "Supported: us-east-1 | eu-west-1 | eu-west-2 | eu-north-1 | eu-central-1 | us-west-2 | random (randomly select region)")
            string(name: "provision_type",
                   defaultValue: "spot",
                   description: "spot|on_demand|spot_fleet")
            string(name: "post_behavior_db_nodes",
                   defaultValue: "destroy",
                   description: "keep|keep-on-failure|destroy")
            string(name: "post_behavior_loader_nodes",
                   defaultValue: "destroy",
                   description: "keep|keep-on-failure|destroy")
            string(name: "post_behavior_monitor_nodes",
                   defaultValue: "destroy",
                   description: "keep|keep-on-failure|destroy")
            string(name: "post_behavior_k8s_cluster",
                   defaultValue: "destroy",
                   description: "keep|keep-on-failure|destroy")
            string(name: "post_behavior_k8s_cluster",
                    defaultValue: "destroy",
                    description: "keep|keep-on-failure|destroy")
        }
        stages {
            stage("Trigger Tests") {
                steps {
                    script {
                        def myparams = params.collect{
                            string(name: it.key, value: it.value)
                        }
                        for (job in jobs) {
                            build job: "..${job}", wait: false, propagate: false, parameters: myparams
                        }
                    }
                }
            }
        }
    }
}
