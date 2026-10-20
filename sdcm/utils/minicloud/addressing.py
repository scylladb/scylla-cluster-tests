"""Guest network layout of the minicloud backends.

Stdlib only, so the provisioning code can import it at module level: config.py would bring an
import cycle through sct_config.
"""

# Emulated guest networks must not overlap real cloud networks: the host may itself sit in a
# real VPC (an sct-runner always does). An overlapping CIDR sends guest traffic out eth0
# instead of the minicloud TUN, and a blanket 10.0.0.0/8 route cuts the host off from Argus.
#
# So all guests live in 10.160.0.0/11, which minicloud-setup.sh routes into the TUN together
# with the emulator's default VPC 172.31.0.0/16 (scylladb/minicloud#187):
#
#   * AWS:   10.160.0.0/16 .. 10.175.0.0/16, AwsRegion adds 160 to the region index.
#   * GCE:   10.176.0.0/16 .. 10.179.0.0/16, qa-vpc subnets from prepare_gce_network()
#            (GCE auto-mode would use the unrouted 10.128.0.0/9).
#   * Azure: 10.180.0.0/16 .. 10.183.0.0/16, one VNet per MINICLOUD_AZURE_REGIONS entry.
#
# The TUN address stays outside every routed range.
MINICLOUD_REGION_INDEX_OFFSET = 160
MINICLOUD_TUN_ADDR = "10.127.0.1/24"
MINICLOUD_HOST_VPC_ROUTES = ("10.160.0.0/11", "172.31.0.0/16")
MINICLOUD_GCE_NETWORK = "qa-vpc"
MINICLOUD_GCE_REGION_INDEX_OFFSET = 176
# Same regions as getJenkinsLabels. Only append: the order sets each subnet index.
MINICLOUD_GCE_REGIONS = ("us-east1", "us-east4", "us-west1", "us-central1")
MINICLOUD_GCE_SUBNET_CIDR_TMPL = "10.{}.0.0/16"
MINICLOUD_AZURE_REGION_INDEX_OFFSET = 180
# The order defines each region VNet index, so only append.
MINICLOUD_AZURE_REGIONS = ("eastus", "eastus2", "westus2", "northeurope")


def azure_vnet_address_space(region: str) -> str:
    """Return the minicloud VNet /16 of the region; each region gets its own."""
    try:
        index = MINICLOUD_AZURE_REGIONS.index(region)
    except ValueError:
        raise ValueError(
            f"region '{region}' is not supported by the minicloud azure backend, "
            f"supported regions: {MINICLOUD_AZURE_REGIONS}"
        ) from None

    return f"10.{MINICLOUD_AZURE_REGION_INDEX_OFFSET + index}.0.0/16"
