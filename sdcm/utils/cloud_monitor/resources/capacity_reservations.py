from dataclasses import dataclass
from datetime import datetime
from logging import getLogger

import boto3

from sdcm.utils.common import all_aws_regions

LOGGER = getLogger(__name__)


@dataclass
class CapacityReservation:
    id: str
    instance_type: str
    region: str
    capacity_total: int
    capacity_available: int
    start_time: datetime
    user: str
    is_unused: bool


def get_active_capacity_reservations() -> list[CapacityReservation]:
    reservations = []
    LOGGER.info("Getting AWS Capacity Reservations...")
    try:
        for region in all_aws_regions():
            LOGGER.info("Getting AWS Capacity Reservations in %s...", region)
            client = boto3.client('ec2', region_name=region)
            response = client.describe_capacity_reservations(
                Filters=[
                    {
                        'Name': 'state',
                        'Values': ['active']
                    }]
            )
            for cr in response['CapacityReservations']:
                user = next((tag['Value'] for tag in cr.get('Tags', []) if tag['Key'] == 'RunByUser'), 'N/A')
                reservations.append(CapacityReservation(
                    id=cr["CapacityReservationId"],
                    instance_type=cr["InstanceType"],
                    region=cr["AvailabilityZone"],
                    capacity_total=cr["TotalInstanceCount"],
                    capacity_available=cr["AvailableInstanceCount"],
                    start_time=cr["StartDate"],
                    user=user,
                    is_unused=cr["AvailableInstanceCount"] == cr["TotalInstanceCount"]
                ))
        LOGGER.info("Found total %d active capacity reservations", len(reservations))
    except Exception as e:  # noqa: BLE001
        LOGGER.error("Failed to get capacity reservations: %s", e)
    return reservations
