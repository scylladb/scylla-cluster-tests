import logging

from sdcm.utils.k8s import HelmValues, get_helm_pool_affinity_values

LOGGER = logging.getLogger(__name__)


class ChaosMesh:  # pylint: disable=too-few-public-methods
    NAMESPACE = "chaos-mesh"
    VERSION = "2.5.0"
    HELM_SETTINGS = {
        'dashboard.create': True,
        'dnsServer.create': True
    }

    def __init__(self, k8s_cluster: "KubernetesCluster"):
        self._k8s_cluster = k8s_cluster

    def initialize(self) -> None:
        """Installs chaos-mesh on k8s cluster and prepares for future k8s chaos testing."""
        if self._k8s_cluster.kubectl(f"get ns {self.NAMESPACE}", ignore_status=True).ok:
            LOGGER.info("Chaos Mesh is already installed. Skipping installation.")
            return
        LOGGER.info("Installing chaos-mesh on %s k8s cluster...", self._k8s_cluster.k8s_scylla_cluster_name)
        self._k8s_cluster.helm("repo add chaos-mesh https://charts.chaos-mesh.org")
        self._k8s_cluster.helm('repo update')
        self._k8s_cluster.kubectl(f"create namespace {self.NAMESPACE}")
        aux_node_pool_affinity = get_helm_pool_affinity_values(
            self._k8s_cluster.POOL_LABEL_NAME, self._k8s_cluster.AUXILIARY_POOL_NAME)
        scylla_node_pool_affinity = get_helm_pool_affinity_values(
            self._k8s_cluster.POOL_LABEL_NAME, self._k8s_cluster.SCYLLA_POOL_NAME)
        self._k8s_cluster.helm_install(
            target_chart_name="chaos-mesh",
            source_chart_name="chaos-mesh/chaos-mesh",
            version=self.VERSION,
            use_devel=False,
            namespace=self.NAMESPACE,
            values=HelmValues(self.HELM_SETTINGS | {
                "chaosDaemon": scylla_node_pool_affinity,
                "controllerManager": aux_node_pool_affinity,
                "dnsServer": aux_node_pool_affinity
            }),
            atomic=True,
            timeout="30m"
        )
        LOGGER.info("chaos-mesh installed successfully on %s k8s cluster.", self._k8s_cluster.k8s_scylla_cluster_name)
