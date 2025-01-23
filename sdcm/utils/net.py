"""
Utility functions related to network information
"""
import ipaddress

import os
import socket
import logging

import requests

LOGGER = logging.getLogger(__name__)


def get_sct_runner_ip() -> str:
    return os.environ.get("RUNNER_IP", "127.0.0.1")


def get_my_ip():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.connect(("8.8.8.8", 80))
    ip = sock.getsockname()[0]
    sock.close()
    return ip


def get_my_public_ip() -> str:
    hostnames = ['https://checkip.amazonaws.com', 'https://api4.ipify.org',
                 'https://v4.ident.me/', 'https://myip.dnsomatic.com']

    for idx, hostname in enumerate(hostnames):
        try:
            result = requests.get(hostname, timeout=10)
            result.raise_for_status()

            ip_address = result.text.strip()
            ipaddress.IPv4Address(ip_address)  # validating that we got IPv4 address
            return ip_address
        except (ipaddress.AddressValueError, ipaddress.NetmaskValueError, requests.exceptions.RequestException):
            LOGGER.warning("Failed to get my public IP from %s", hostname)

    raise ValueError("Failed to get my public IP from any service")


def resolve_ip_to_dns(ip_address: str) -> str:
    try:
        return socket.gethostbyaddr(ip_address)[0]
    except socket.herror as e:
        raise ValueError(f"Unable to resolve IP: {e}")
