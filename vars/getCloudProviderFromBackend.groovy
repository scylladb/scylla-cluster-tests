
def call(String backend) {
    def backend_to_provider = [
        'k8s-eks': 'aws',
        'k8s-gke': 'gce',
        'k8s-local-kind-aws': 'aws',
        'k8s-local-kind-gce': 'gce',
        'aws-siren': 'aws',
        'gce-siren': 'gce',
        'azure': 'azure'
        ]
    if (!backend) {
        return backend
    }
    def cloud_provider = backend.trim().toLowerCase()
    return backend_to_provider.get(cloud_provider, cloud_provider)
    }
