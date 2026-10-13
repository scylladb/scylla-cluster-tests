"""Generate docs/sct_overview.png, the high-level SCT architecture diagram shown in README.md.

Needs graphviz, so run it in a throwaway container from the repo root:

    docker run --rm -v "$PWD/docs:/docs" -w /docs python:3.14-slim sh -c \
        "apt-get update -qq && apt-get install -y -qq graphviz >/dev/null && pip install -q diagrams && python sct_overview.py"
"""

from pathlib import Path

import diagrams
from diagrams import Cluster, Diagram, Edge
from diagrams.custom import Custom
from diagrams.generic.compute import Rack
from diagrams.onprem.ci import Jenkins
from diagrams.onprem.client import User
from diagrams.onprem.compute import Server
from diagrams.onprem.container import Docker
from diagrams.onprem.database import Scylla
from diagrams.onprem.monitoring import Grafana, Prometheus
from diagrams.programming.language import Python

ICONS = Path(diagrams.__file__).parent.parent / "resources"


def logo(label, path):
    return Custom(label, str(ICONS / path))


GRAPH = {"fontsize": "22", "pad": "0.4", "nodesep": "0.8", "ranksep": "0.9", "splines": "spline", "compound": "true"}
BACKENDS_LABEL = "Pluggable backends: where the machines come from"
RUN_LABEL = "Created for each test run"
BACKENDS, RUN = f"cluster_{BACKENDS_LABEL}", f"cluster_{RUN_LABEL}"  # graphviz ids, for edges that end at a box
TALL = {"height": "2.3"}  # room for the label under tall icons

with Diagram(
    "Scylla Cluster Tests (SCT)", filename="sct_overview", outformat="png", show=False, direction="TB", graph_attr=GRAPH
):
    with Cluster("Who starts a run"):
        dev = User("developer laptop")
        jenkins = Jenkins("Jenkins + SCT runner")

    with Cluster("SCT framework, running in the Hydra container"):
        tests = Python("tests\nlongevity · perf · upgrade\nartifacts · manager · ...")
        nemesis = Python("nemesis engine\nfailure injection")
        stress = Python("stress tool drivers")

    argus = Server("Argus\nresults · events · logs")

    with Cluster(BACKENDS_LABEL):
        # graphviz lays out a cluster's nodes right-to-left, so list them reversed
        backends = [
            Rack("bare metal"),
            Scylla("ScyllaDB Cloud", **TALL),
            logo("Kubernetes\nEKS · GKE · kind", "k8s/k8s.png"),
            Docker("Docker"),
            logo("OCI", "oci/oci.png"),
            logo("Azure", "azure/azure.png"),
            logo("GCE", "gcp/gcp.png"),
            logo("AWS", "aws/aws.png"),
        ]

    with Cluster(RUN_LABEL):
        with Cluster("Loaders"):
            loaders = Server("cassandra-stress · latte\nscylla-bench · YCSB · gemini")
        with Cluster("DB cluster under test"):
            db = Scylla("ScyllaDB nodes", **TALL)
        with Cluster("Monitor (scylla-monitoring)"):
            prometheus = Prometheus("Prometheus")
            grafana = Grafana("Grafana")

    [dev, jenkins] >> Edge(label="hydra run-test") >> tests
    tests >> Edge(label="report", style="dashed") >> argus
    tests >> Edge(label="provision", lhead=BACKENDS) >> backends[4]
    backends[3] >> Edge(label="VMs · containers · pods", ltail=BACKENDS, lhead=RUN) >> db
    side = {"constraint": "false"}
    stress >> Edge(label="stress commands") >> loaders
    loaders >> Edge(label="CQL / Alternator load", **side) >> db
    nemesis >> Edge(label="inject failures", color="firebrick", style="bold") >> db
    db << Edge(label="scrape metrics", style="dashed", **side) << prometheus
    prometheus >> Edge(**side) >> grafana
