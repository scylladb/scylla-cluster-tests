import json
from json.decoder import JSONDecodeError
from pathlib import Path
import click
import logging

from argus.client.base import TestStatus
from argus.client.generic.client import ArgusGenericClient

LOGGER = logging.getLogger(__name__)

def validate_extra_headers(ctx, param, value):
    if isinstance(value, dict):
        return value

    try:
        return json.loads(value)
    except JSONDecodeError as ex:
        raise click.BadParameter(f"--extra-headers should be in json format:\n\n{value}\n\n{ex}")


@click.group
def cli():
    pass


@click.command("submit")
@click.option("--api-key", help="Argus API key for authorization", required=True, envvar='ARGUS_AUTH_TOKEN')
@click.option("--base-url", default="https://argus.scylladb.com", help="Base URL for argus instance")
@click.option("--id", required=True, help="UUID (v4 or v1) unique to the job")
@click.option("--build-id", required=True, help="Unique job identifier in the build system, e.g. scylla-master/group/job for jenkins (The full path)")
@click.option("--build-url", required=True, help="Job URL in the build system")
@click.option("--started-by", required=True, help="Username of the user who started the job")
@click.option("--scylla-version", required=False, default=None, help="Version of Scylla used for this job")
@click.option("--extra-headers", default={}, type=click.UNPROCESSED, callback=validate_extra_headers, help="extra headers to pass to argus, should be in json format", envvar='ARGUS_EXTRA_HEADERS')
def submit_run(api_key: str, base_url: str, id: str, build_id: str, build_url: str, started_by: str, scylla_version: str = None, extra_headers: dict | None = None):
    LOGGER.info("Submitting %s (%s) to Argus...", build_id, id)
    client = ArgusGenericClient(auth_token=api_key, base_url=base_url, extra_headers=extra_headers)
    client.submit_generic_run(build_id=build_id, run_id=id, started_by=started_by, build_url=build_url, scylla_version=scylla_version)
    LOGGER.info("Done.")


@click.command("finish")
@click.option("--api-key", help="Argus API key for authorization", required=True, envvar='ARGUS_AUTH_TOKEN')
@click.option("--base-url", default="https://argus.scylladb.com", help="Base URL for argus instance")
@click.option("--id", required=True, help="UUID (v4 or v1) unique to the job")
@click.option("--status", required=True, help="Resulting job status")
@click.option("--scylla-version", required=False, default=None, help="Version of Scylla used for this job")
@click.option("--extra-headers", default={}, type=click.UNPROCESSED, callback=validate_extra_headers, help="extra headers to pass to argus, should be in json format", envvar='ARGUS_EXTRA_HEADERS')
def finish_run(api_key: str, base_url: str, id: str, status: str, scylla_version: str = None, extra_headers: dict | None = None):
    client = ArgusGenericClient(auth_token=api_key, base_url=base_url, extra_headers=extra_headers)
    status = TestStatus(status)
    client.finalize_generic_run(run_id=id, status=status, scylla_version=scylla_version)


@click.command("trigger-jobs")
@click.option("--api-key", help="Argus API key for authorization", required=True, envvar='ARGUS_AUTH_TOKEN')
@click.option("--base-url", default="https://argus.scylladb.com", help="Base URL for argus instance")
@click.option("--version", help="Scylla version to filter plans by", default=None, required=False)
@click.option("--plan-id", help="Specific plan id for filtering", default=None, required=False)
@click.option("--release", help="Release name to filter plans by", default=None, required=False)
@click.option("--job-info-file", required=True, help="JSON file with trigger information (see detailed docs)")
@click.option("--extra-headers", default={}, type=click.UNPROCESSED, callback=validate_extra_headers, help="extra headers to pass to argus, should be in json format", envvar='ARGUS_EXTRA_HEADERS')
def trigger_jobs(api_key: str, base_url: str, job_info_file: str, version: str, plan_id: str, release: str, extra_headers: dict | None = None):
    client = ArgusGenericClient(auth_token=api_key, base_url=base_url, extra_headers=extra_headers)
    path = Path(job_info_file)
    if not path.exists():
        LOGGER.error("File not found: %s", job_info_file)
        exit(128)
    payload = json.load(path.open("rt", encoding="utf-8"))
    client.trigger_jobs({ "release": release, "version": version, "plan_id": plan_id, **payload })


cli.add_command(submit_run)
cli.add_command(finish_run)


if __name__ == "__main__":
    cli()
