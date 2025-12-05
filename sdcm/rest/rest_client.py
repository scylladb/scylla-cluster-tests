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
# Copyright (c) 2022 ScyllaDB

import logging
from functools import cached_property
from typing import Dict, Literal
from urllib.parse import urljoin

import requests
from requests import Response


LOGGER = logging.getLogger(__name__)


class RestClient:
    def __init__(self, host: str, endpoint: str):
        self._url_prefix = "http://"
        self._host = host
        self._endpoint = endpoint

    @cached_property
    def _base_url(self) -> str:
        return urljoin(f"{self._url_prefix}{self._host}", self._endpoint)

    def get(self, path: str, params: Dict[str, str] = None) -> Response:
        url = f"{self._base_url}/{path}"
        LOGGER.info("Sending a GET request for: %s", url)

        return requests.get(url=url, params=params)

    def post(self, path: str, params: Dict[str, str] = None) -> Response:
        url = f"{self._base_url}/{path}"
        LOGGER.info("Sending a POST request for: %s", url)
        return requests.post(url=url, params=params)

    def _prepare_request(self, method: Literal["GET", "POST"], path: str, params: dict[str, str]):
        full_url = f"{self._base_url}/{path}"
        prepared_request = requests.Request(method=method, url=full_url, params=params).prepare()

        return prepared_request
