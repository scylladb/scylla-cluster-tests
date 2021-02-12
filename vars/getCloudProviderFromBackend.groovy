
def call(String backend) {
    def backend_to_provider = [
        'k8s-eks': 'aws',
        'k8s-gke': 'gce',
        'k8s-gce-minikube': 'gce',
        ]
    def cloud_provider = backend.trim().toLowerCase()
    return backend_to_provider.get(cloud_provider, cloud_provider)
    }
