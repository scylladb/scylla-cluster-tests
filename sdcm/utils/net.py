"""
Utility functions related to network information
"""
import ipaddress

import os
import socket
import requests


def get_sct_runner_ip() -> str:
    return os.environ.get("RUNNER_IP", "127.0.0.1")


def get_my_ip():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.connect(("8.8.8.8", 80))
    ip = sock.getsockname()[0]
    sock.close()
    return ip


def get_my_public_ip() -> str:
    hostnames = ['https://api4.my-ip.io/ip', 'https://api.ipify.org', 'https://ip4.seeip.org/ip',
                 'http://ipv4.icanhazip.com']

    ip_address = "[Not Found]"
    for idx, hostname in enumerate(hostnames):
        try:
            result = requests.get(hostname, timeout=10)
            result.raise_for_status()

            ip_address = result.text.strip()
            ipaddress.ip_address(ip_address)  # validating that we got IP address
        except (ValueError, requests.exceptions.RequestException):
            if idx == len(hostnames) - 1:
                raise
    return ip_address
