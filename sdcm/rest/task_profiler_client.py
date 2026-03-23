# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

from fabric.runners import Result

from sdcm.rest.remote_curl_client import RemoteCurlClient


class TaskProfilerClient(RemoteCurlClient):
    def __init__(self, node: "BaseNode"):  # noqa F821
        super().__init__(host="localhost:10000", endpoint="system", node=node)

    def start(self, sampling_interval_ms: int = 100, timeout: int = 60) -> Result:
        return self.run_remoter_curl(
            method="POST",
            path="task_profiler/start",
            params={"sampling_interval_ms": str(sampling_interval_ms)},
            timeout=timeout,
            retry=0,  # no-retry: POST is non-idempotent; a retried start could double-start the profiler
        )

    def stop(self, filename: str, timeout: int = 300) -> Result:
        return self.run_remoter_curl(
            method="POST",
            path="task_profiler/stop",
            params={"filename": filename},
            timeout=timeout,
            retry=0,  # no-retry: POST is non-idempotent; retrying stop could overwrite/duplicate dump files
        )
