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
# Copyright (c) 2023 ScyllaDB

from enum import Enum


class RequestMethods(Enum):
    GET = 'GET'
    PUT = 'PUT'
    DELETE = 'DELETE'
    POST = 'POST'


def build_node_api_command(path_url, request_method: RequestMethods = RequestMethods.GET, api_port=10000, silent=True):
    if not path_url.startswith('/'):
        path_url = '/' + path_url
    silent_flag = '-s ' if silent else ''

    return f'curl {silent_flag}-X {request_method.value} --header "Content-Type: application/json" --header ' \
        f'"Accept: application/json" "http://127.0.0.1:{api_port}{path_url}"'
