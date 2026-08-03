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
# Copyright (c) 2021 ScyllaDB

SPOT_CNT_LIMIT = 10
# Limit of instances that AWS API can handle with single spot request

SPOT_REQUEST_TIMEOUT = 300
# Time we wait spot instance to be fulfilled

SPOT_REQUEST_WAITING_TIME = 5
# How much time we wait before getting status of spot/fleet request

STATUS_FULFILLED = "fulfilled"
# Spot request status that is signaling that it has been processed and fulfilled

SPOT_PRICE_TOO_LOW = "price-too-low"
# Spot request status that is signaling that it won't be processed because price you want is too low

SPOT_CAPACITY_NOT_AVAILABLE_ERROR = "capacity-not-available"
# Spot request event type that is signaling that it won't be processed due to the lack of resources on AWS side

EC2_FLEET_LIMIT = 500
# Limit of instances that AWS API can handle with single EC2 Fleet request

EC2_FLEET_TYPE_INSTANT = "instant"
# EC2 Fleet request type that provisions synchronously and does not try to maintain target capacity.
# SCT owns node lifecycle (nemesis terminates nodes on purpose), so automatic replacement must stay off.

EC2_FLEET_ALLOCATION_STRATEGY = "capacity-optimized"
# Allocation strategy telling AWS to pick the instance pools with the deepest spare capacity,
# which is what reduces interruption rate when several instance types are offered.

EC2_FLEET_UNFULFILLABLE_ERROR_CODES = (
    "InsufficientInstanceCapacity",
    "InsufficientHostCapacity",
    "SpotMaxPriceTooLow",
    "MaxSpotInstanceCountExceeded",
    "InstanceLimitExceeded",
    "UnfulfillableCapacity",
)
# `create_fleet` Errors[].ErrorCode values that mean the request can never be fulfilled as-is.
# EC2 Fleet has no `describe_spot_fleet_request_history` equivalent, so these replace the
# FLEET_LIMIT_EXCEEDED_ERROR / SPOT_CAPACITY_NOT_AVAILABLE_ERROR event subtypes used by Spot Fleet.
