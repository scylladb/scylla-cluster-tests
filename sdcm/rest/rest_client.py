from typing import Dict, Literal

import requests
from requests import Response

from sct import LOGGER


class RestClient:
    def __init__(self,
                 host: str,
                 endpoint: str):
        self._url_prefix = "http://"
        self._host = host
        self._endpoint = endpoint

    def prepare_get_request(self, path: str, params: dict[str, str]):
        prepared_request = self._prepare_request(method="GET", path=path, params=params)

        return prepared_request

    def get(self, path: str, params: Dict[str, str] = None) -> Response:
        url = self._get_full_url(path)
        LOGGER.info("Sending a GET request for: %s", url)

        return requests.get(url=url, params=params)

    def prepare_post_request(self, path: str, params: dict[str, str]):
        prepared_request = self._prepare_request(method="POST", path=path, params=params)

        return prepared_request

    def post(self, path: str, params: Dict[str, str] = None) -> Response:
        url = self._get_full_url(path)
        LOGGER.info("Sending a POST request for: %s", url)
        requests.Request()
        return requests.post(url=url, params=params)

    def _get_full_url(self, path: str) -> str:
        full_url = f"{self._url_prefix}{self._host}/{self._endpoint}/{path}"

        return full_url

    def _prepare_request(self, method: Literal["GET", "POST"], path: str, params: dict[str, str]):
        full_url = f"{self._url_prefix}{self._host}/{self._endpoint}/{path}"
        prepared_request = requests.Request(method=method, url=full_url, params=params).prepare()

        return prepared_request
