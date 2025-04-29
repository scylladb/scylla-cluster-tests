from typing import Literal

from sdcm.cluster import BaseNode


class ScyllaMetricsController:
    """Class to control Scylla metrics using API. Ref: https://github.com/scylladb/scylladb/pull/12670
    issue about missing docs: https://github.com/scylladb/scylla-monitoring/issues/2196"""
    curl_cmd = "curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json'"
    endpoint = "http://localhost:10000/v2/metrics-config/"

    @classmethod
    def modify_scylla_metrics(cls, nodes: list[BaseNode],
                              action: Literal["drop", "keep"] = "drop",
                              target_label: str = "level",
                              regex: str = ".*"
                              ) -> None:
        """Disables/Enables metrics based on the action provided.
        When specifying regex, first part of metric 'scylla_' is ignored, so e.g. use 'transport_.*' to keep/drop transport metrics."""
        payload = f'[{{"source_labels": ["__name__"], "action": "{action}", "target_label": "{target_label}", "regex": "{regex}"}}]'
        for node in nodes:
            node.remoter.run(f'{cls.curl_cmd} -d \'{payload}\' {cls.endpoint}')
