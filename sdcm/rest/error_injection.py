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
from typing import TYPE_CHECKING

from sdcm.rest.remote_curl_client import RemoteCurlClient
from sdcm.remote.libssh2_client import Result

if TYPE_CHECKING:
    from sdcm.cluster import BaseNode


class ErrorInjection(RemoteCurlClient):
    """
    A client for managing error injection in ScyllaDB via REST API.
    This class provides methods to interact with the error injection endpoint,
    allowing users to list, enable, disable, and manage injected errors.

    The ErrorInjection have to be used only with dev builds and
    for the special rare cases, because SCT tests are designed to test
    production grade ScyllaDB clusters.
    """

    def __init__(self, node: "BaseNode"):
        super().__init__(host="localhost:10000", endpoint="v2/error_injection/injection", node=node)

    def list_injected_errors(self) -> Result:
        """Retrieves a list of all currently injected error names"""
        return self.run_remoter_curl(method="GET", path="", params=None)

    def get_injected_error_details(self, error_name: str) -> Result:
        """Retrieves detailed information about a specific injected error."""
        return self.run_remoter_curl(method="GET", path=f"{error_name}", params=None)

    def remove_errors(self) -> Result:
        """Disables all currently active error injections."""

        return self.run_remoter_curl(method="DELETE", path="", params=None)

    def inject_error(self, error_name: str, one_shot: bool = False, data: dict[str, str] | None = None) -> Result:
        """Enables a specific error injection with optional configuration"""
        params = {"one_shot": str(one_shot).lower()}
        return self.run_remoter_curl(method="POST", path=f"{error_name}", params=params, data=data)

    def send_message_to_error(self, error_name: str) -> Result:
        """Sends a message to a specific error injection"""
        return self.run_remoter_curl(method="POST", path=f"{error_name}/message", params=None)
