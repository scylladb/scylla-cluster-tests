from functools import lru_cache
from uuid import UUID

from mcp.server.fastmcp import FastMCP
from argus.client.sct.client import ArgusSCTClient

from sdcm.keystore import KeyStore

# Create an MCP server
mcp = FastMCP("Argus")


@lru_cache
def get_argus_client():
    creds = KeyStore().get_argus_rest_credentials()
    # TODO: add headers
    return ArgusSCTClient(auth_token=creds["token"], base_url=creds["baseUrl"], run_id=UUID("e38b303f-df9b-4aac-b9d8-930cfd45306b"))


@mcp.tool()
def get_sct_run_status(test_id: str) -> str:
    """Get the specific sct run status from argus"""
    client = get_argus_client()
    return client.get_status(run_id=UUID(test_id))


@mcp.tool()
def get_sct_run_information(test_id: str) -> dict:
    """Get the specific sct run information from argus"""
    client = get_argus_client()
    results = client.get_run(run_id=UUID(test_id))
    return dict(**results)


@mcp.tool()
def search_sct_run(query: str) -> list[dict]:
    """Search for SCT (Scylla Cluster Tests) runs using Argus search functionality.

    This function allows searching through SCT test runs using various query patterns.
    The search is performed against the Argus database which stores all test results.

    Args:
        query (str): Search query string. Supports various search patterns:
            - Simple text search: "gemini", "alternator"
            - Version search: "release:2024.1"
            - Group search: "group:alternator"
            - Test Id: "e38b303f-df9b-4aac-b9d8-930cfd45306b"

    Returns:
        list: A list containing search results with test runs matching the query.
            Each result includes test ID, name, status, and other metadata.

    Examples:
        >>> # Search for all Gemini tests
        >>> search_sct_run("gemini")

        >>> # Search for specific version tests
        >>> search_sct_run("release:2024.1")

        >>> # Search by multiple criteria
        >>> search_sct_run("gemini release:2024.1")

        >>> # Search for specific test group
        >>> search_sct_run("group:alternator")
    """
    client = get_argus_client()
    client.api_prefix = '/planning'
    results = client.get('/search', params={'query': query}, location_params={})
    return results.json()['response']['hits'][:20]
