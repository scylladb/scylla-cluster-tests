import sys
from pathlib import Path

from sdcm.remote import LOCALRUNNER
from sdcm.utils.git import clone_repo

scylla_qa_internal_path = Path(__file__).resolve().parents[2] / 'scylla-qa-internal'
if not scylla_qa_internal_path.exists():
    print("scylla-qa-internal not found, cloning...")
    try:
        clone_repo(
            remoter=LOCALRUNNER,
            repo_url="git@github.com:scylladb/scylla-qa-internal.git",
            destination_dir_name=str(scylla_qa_internal_path),
            clone_as_root=False,
            branch="master")
        print("Successfully cloned scylla-qa-internal")
    except Exception as exc:  # noqa: BLE001
        print(f"Failed to clone scylla-qa-internal: {exc}")


# Add scylla-qa-internal to the Python path using pathlib
# TODO: make this support multiple paths if needed
scylla_qa_internal_path_str = str(scylla_qa_internal_path)
if scylla_qa_internal_path_str not in sys.path:
    sys.path.insert(0, scylla_qa_internal_path_str)

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
