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

EC2_FLEET_LAUNCH_TEMPLATE_PREFIX = "sct-fleet-"
# Name prefix of the throwaway launch templates backing EC2 Fleet requests. clean-resources only
# deletes launch templates carrying this prefix on the `aws` backend, so it never touches long-lived
# templates such as the SCT runner's.

EC2_FLEET_TYPE_INSTANT = "instant"
# EC2 Fleet request type that provisions synchronously and does not try to maintain target capacity.
# SCT owns node lifecycle (nemesis terminates nodes on purpose), so automatic replacement must stay off.

EC2_FLEET_ALLOCATION_STRATEGY = "capacity-optimized-prioritized"
# Allocation strategy telling AWS to pick the instance pools with the deepest spare capacity (which is
# what reduces the interruption rate when several instance types are offered) while honoring each
# override's Priority on a best-effort basis, so the primary instance type is preferred over the
# alternatives whenever its pool can take the request. Plain `capacity-optimized` has no notion of a
# preferred type and could, e.g., place a scale test on an older generation despite the primary
# having capacity.

EC2_FLEET_RETRYABLE_ERROR_CODES = (
    "RequestLimitExceeded",
    "InternalError",
    "ServiceUnavailable",
)
# `create_fleet` Errors[].ErrorCode values that are transient (throttling, AWS-side failures), so the
# same request may succeed moments later. Anything else - capacity (InsufficientInstanceCapacity),
# account limits (MaxSpotInstanceCountExceeded) or invalid configuration - is not retried; the caller's
# AZ/region/on-demand fallback handles those. EC2 Fleet has no `describe_spot_fleet_request_history`
# equivalent, so this list replaces the event subtypes Spot Fleet reported.

EC2_FLEET_MAX_ATTEMPTS = 3
# Attempts per EC2 Fleet batch when it under-fulfills because of transient errors only.

EC2_FLEET_RETRY_BACKOFF = 10
# Seconds to wait before the next EC2 Fleet attempt, multiplied by the attempt number (10s, then 20s).
