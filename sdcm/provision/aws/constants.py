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

SPOT_CNT_LIMIT = 20
# Limit of instances that AWS API can handle with single spot request

SPOT_FLEET_LIMIT = 50
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
