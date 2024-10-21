import base64
import json
from pathlib import Path
import click
import logging
from argus.common.enums import TestStatus

from argus.client.driver_matrix_tests.client import ArgusDriverMatrixClient

LOGGER = logging.getLogger(__name__)


@click.group
def cli():
    pass


def _submit_driver_result_internal(api_key: str, base_url: str, run_id: str, metadata_path: str):
    metadata = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
    LOGGER.info("Submitting results for %s [%s/%s] to Argus...", run_id, metadata["driver_name"], metadata["driver_type"])
    raw_xml = (Path(metadata_path).parent / metadata["junit_result"]).read_bytes()
    client = ArgusDriverMatrixClient(run_id=run_id, auth_token=api_key, base_url=base_url)
    client.submit_driver_result(driver_name=metadata["driver_name"], driver_type=metadata["driver_type"], raw_junit_data=base64.encodebytes(raw_xml))
    LOGGER.info("Done.")

def _submit_driver_failure_internal(api_key: str, base_url: str, run_id: str, metadata_path: str):
    metadata = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
    LOGGER.info("Submitting failure for %s [%s/%s] to Argus...", run_id, metadata["driver_name"], metadata["driver_type"])
    client = ArgusDriverMatrixClient(run_id=run_id, auth_token=api_key, base_url=base_url)
    client.submit_driver_failure(driver_name=metadata["driver_name"], driver_type=metadata["driver_type"], failure_reason=metadata["failure_reason"])
    LOGGER.info("Done.")


@click.command("submit-run")
@click.option("--api-key", help="Argus API key for authorization", required=True)
@click.option("--base-url", default="https://argus.scylladb.com", help="Base URL for argus instance")
@click.option("--id", "run_id", required=True, help="UUID (v4 or v1) unique to the job")
@click.option("--build-id", required=True, help="Unique job identifier in the build system, e.g. scylla-master/group/job for jenkins (The full path)")
@click.option("--build-url", required=True, help="Job URL in the build system")
def submit_driver_matrix_run(api_key: str, base_url: str, run_id: str, build_id: str, build_url: str):
    LOGGER.info("Submitting %s (%s) to Argus...", build_id, run_id)
    client = ArgusDriverMatrixClient(run_id=run_id, auth_token=api_key, base_url=base_url)
    client.submit_driver_matrix_run(job_name=build_id, job_url=build_url)
    LOGGER.info("Done.")


@click.command("submit-driver")
@click.option("--api-key", help="Argus API key for authorization", required=True)
@click.option("--base-url", default="https://argus.scylladb.com", help="Base URL for argus instance")
@click.option("--id", "run_id", required=True, help="UUID (v4 or v1) unique to the job")
@click.option("--metadata-path", required=True, help="Path to the metadata .json file that contains path to junit xml and other required information")
def submit_driver_result(api_key: str, base_url: str, run_id: str, metadata_path: str):
    _submit_driver_result_internal(api_key=api_key, base_url=base_url, run_id=run_id, metadata_path=metadata_path)


@click.command("fail-driver")
@click.option("--api-key", help="Argus API key for authorization", required=True)
@click.option("--base-url", default="https://argus.scylladb.com", help="Base URL for argus instance")
@click.option("--id", "run_id", required=True, help="UUID (v4 or v1) unique to the job")
@click.option("--metadata-path", required=True, help="Path to the metadata .json file that contains path to junit xml and other required information")
def submit_driver_failure(api_key: str, base_url: str, run_id: str, metadata_path: str):
    _submit_driver_failure_internal(api_key=api_key, base_url=base_url, run_id=run_id, metadata_path=metadata_path)


@click.command("submit-or-fail-driver")
@click.option("--api-key", help="Argus API key for authorization", required=True)
@click.option("--base-url", default="https://argus.scylladb.com", help="Base URL for argus instance")
@click.option("--id", "run_id", required=True, help="UUID (v4 or v1) unique to the job")
@click.option("--metadata-path", required=True, help="Path to the metadata .json file that contains path to junit xml and other required information")
def submit_or_fail_driver(api_key: str, base_url: str, run_id: str, metadata_path: str):
    metadata = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
    if metadata.get("failure_reason"):
        _submit_driver_failure_internal(api_key=api_key, base_url=base_url, run_id=run_id, metadata_path=metadata_path)
    else:
        _submit_driver_result_internal(api_key=api_key, base_url=base_url, run_id=run_id, metadata_path=metadata_path)


@click.command("submit-env")
@click.option("--api-key", help="Argus API key for authorization", required=True)
@click.option("--base-url", default="https://argus.scylladb.com", help="Base URL for argus instance")
@click.option("--id", "run_id", required=True, help="UUID (v4 or v1) unique to the job")
@click.option("--env-path", required=True, help="Path to the Build-00.txt file that contains environment information about Scylla")
def submit_driver_env(api_key: str, base_url: str, run_id: str, env_path: str):
    LOGGER.info("Submitting environment for run %s to Argus...", run_id)
    raw_env = Path(env_path).read_text()
    client = ArgusDriverMatrixClient(run_id=run_id, auth_token=api_key, base_url=base_url)
    client.submit_env(raw_env)
    LOGGER.info("Done.")


@click.command("finish-run")
@click.option("--api-key", help="Argus API key for authorization", required=True)
@click.option("--base-url", default="https://argus.scylladb.com", help="Base URL for argus instance")
@click.option("--id", "run_id", required=True, help="UUID (v4 or v1) unique to the job")
@click.option("--status", required=True, help="Resulting job status")
def finish_driver_matrix_run(api_key: str, base_url: str, run_id: str, status: str):
    client = ArgusDriverMatrixClient(run_id=run_id, auth_token=api_key, base_url=base_url)
    client.finalize_run(run_type=ArgusDriverMatrixClient.test_type, run_id=run_id, body={"status": TestStatus(status)})


cli.add_command(submit_driver_matrix_run)
cli.add_command(submit_driver_result)
cli.add_command(submit_or_fail_driver)
cli.add_command(submit_driver_failure)
cli.add_command(submit_driver_env)
cli.add_command(finish_driver_matrix_run)


if __name__ == "__main__":
    cli()
