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

SPOT_FLEET_LIMIT = 500
# Limit of instances that AWS API can handle with single fleet request

SPOT_REQUEST_TIMEOUT = 300
# Time we wait spot instance to be fulfilled

SPOT_REQUEST_WAITING_TIME = 5
# How much time we wait before getting status of spot/fleet request

STATUS_FULFILLED = "fulfilled"
# Spot request status that is signaling that it has been processed and fulfilled

SPOT_STATUS_UNEXPECTED_ERROR = "error"
# Spot activity status that is signaling that something wrong happened while spot request is being processed

SPOT_PRICE_TOO_LOW = "price-too-low"
# Spot request status that is signaling that it won't be processed because price you want is too low

FLEET_LIMIT_EXCEEDED_ERROR = "spotInstanceCountLimitExceeded"
# Spot request event type that is signaling that it won't be processed due to the reaching AWS spot instance limit

SPOT_CAPACITY_NOT_AVAILABLE_ERROR = "capacity-not-available"
# Spot request event type that is signaling that it won't be processed due to the lack of resources on AWS side

SPOT_FLEET_ALLOCATION_STRATEGY = "capacityOptimized"
# Allocation strategy for spot fleet requests. `ec2:GetSpotPlacementScores` documents that a high per-AZ score
# "assumes that your fleet request will be configured to use a single Availability Zone and the
# capacity-optimized allocation strategy" - so this must match, or the scores we rank AZs by over-predict
# our actual fulfillment rate.
#
# NOTE the spelling: RequestSpotFleet's SpotFleetRequestConfigData.AllocationStrategy takes camelCase
# ('capacityOptimized'). The hyphenated 'capacity-optimized' the user guide shows belongs to the *different*
# CreateFleet API (SpotOptionsRequest). botocore does not validate enum values client-side, so the wrong
# spelling reaches AWS and comes back as InvalidParameterValue - failing the fleet path outright.

SPOT_PLACEMENT_SCORE_MAX_REGIONS = 10
# `ec2:GetSpotPlacementScores` accepts at most 10 entries in RegionName.N, so region lists are chunked

SPOT_PLACEMENT_SCORE_RECOMMENDED_TYPES = 3
# AWS: "We recommend that you specify at least three instance types. If you specify one or two instance types,
# or specify variations of a single instance type, the returned placement score will always be low."

SPOT_PLACEMENT_SCORE_CACHE_TTL = 3600
# Placement scores are refreshed by AWS roughly daily, so caching them for an hour is safe and avoids
# re-querying once per AZ candidate within a single provisioning run
