# Re-exported from its new home so this module stays the import site for existing callers.
# It moved because `cloud_catalog.pricing` needs it, and reaching into `cloud_monitor` for it
# created an import cycle via this package's eagerly-importing `__init__.py`.
from sdcm.utils.cloud_catalog.lifecycle import InstanceLifecycle

NA = "N/A"

__all__ = ["InstanceLifecycle", "NA"]
