import sys
from pathlib import Path

# Add scylla-qa-internal to the Python path using pathlib
# TODO: make this support multiple paths if needed
scylla_qa_internal_path = str((Path(__file__).parent.parent.parent / 'scylla-qa-internal').resolve())
if scylla_qa_internal_path not in sys.path:
    sys.path.insert(0, scylla_qa_internal_path)

# Import the internal modules
try:
    from xcloud_internal.connectivity import XCloudConnectivityContainerMixin
except ImportError:
    not_supported_message = (
        "XCloud connectivity is not available in this environment. "
        "Ensure that the xcloud_internal module is installed and accessible."
    )
    # If xcloud_internal is not available, define a dummy mixin

    class XCloudConnectivityContainerMixin:
        def xcloud_connect_container_run_args(self):
            raise NotImplementedError(not_supported_message)

        def xcloud_connect_wait_to_be_ready(self):
            pass

        @staticmethod
        def xcloud_connect_supported(params):
            return False

        @property
        def xcloud_connect(self):
            raise NotImplementedError(not_supported_message)

        def xcloud_connect_get_ssh_address(self, node):
            raise NotImplementedError(not_supported_message)

__all__ = ["XCloudConnectivityContainerMixin"]
