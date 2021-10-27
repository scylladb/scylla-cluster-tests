"""
Utility functions related to network information
"""
import os
import socket


def get_sct_runner_ip() -> str:
    return os.environ.get("RUNNER_IP", "127.0.0.1")


def get_my_ip():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.connect(("8.8.8.8", 80))
    ip = sock.getsockname()[0]
    sock.close()
    return ip
