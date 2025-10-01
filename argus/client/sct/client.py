import base64
from typing import Any
from uuid import UUID
from dataclasses import asdict
from argus.common.sct_types import GeminiResultsRequest, PerformanceResultsRequest, RawEventPayload
from argus.common.enums import ResourceState, TestStatus
from argus.client.base import ArgusClient
from argus.client.sct.types import EventsInfo, LogLink, Package


class ArgusSCTClient(ArgusClient):
    test_type = "scylla-cluster-tests"
    schema_version: None = "v8"

    class Routes(ArgusClient.Routes):
        SUBMIT_PACKAGES = "/sct/$id/packages/submit"
        SUBMIT_SCREENSHOTS = "/sct/$id/screenshots/submit"
        CREATE_RESOURCE = "/sct/$id/resource/create"
        TERMINATE_RESOURCE = "/sct/$id/resource/$name/terminate"
        UPDATE_RESOURCE = "/sct/$id/resource/$name/update"
        SET_SCT_RUNNER = "/sct/$id/sct_runner/set"
        UPDATE_SHARDS_FOR_RESOURCE = "/sct/$id/resource/$name/shards"
        SUBMIT_NEMESIS = "/sct/$id/nemesis/submit"
        SUBMIT_GEMINI_RESULTS = "/sct/$id/gemini/submit"
        SUBMIT_PERFORMANCE_RESULTS = "/sct/$id/performance/submit"
        FINALIZE_NEMESIS = "/sct/$id/nemesis/finalize"
        SUBMIT_EVENTS = "/sct/$id/events/submit"
        SUBMIT_EVENT = "/sct/$id/event/submit"
        SUBMIT_JUNIT_REPORT = "/sct/$id/junit/submit"

    def __init__(self, run_id: UUID, auth_token: str, base_url: str, api_version="v1", extra_headers: dict | None = None) -> None:
        super().__init__(auth_token, base_url, api_version, extra_headers=extra_headers)
        self.run_id = run_id

    def submit_sct_run(self, job_name: str, job_url: str, started_by: str, commit_id: str,
                       origin_url: str, branch_name: str, sct_config: dict) -> None:
        """
            Submits an SCT run to argus.
        """
        response = super().submit_run(run_type=self.test_type, run_body={
            "run_id": str(self.run_id),
            "job_name": job_name,
            "job_url": job_url,
            "started_by": started_by,
            "commit_id": commit_id,
            "origin_url": origin_url,
            "branch_name": branch_name,
            "sct_config": sct_config,
        })

        self.check_response(response)

    def set_sct_run_status(self, new_status: TestStatus) -> None:
        """
            Sets an SCT run's status.
        """
        response = super().set_status(run_type=self.test_type, run_id=self.run_id, new_status=new_status)
        self.check_response(response)

    def set_sct_runner(self, public_ip: str, private_ip: str, region: str, backend: str, name: str = None) -> None:
        """
            Sets runner information for an SCT run.
        """
        response = self.post(
            endpoint=self.Routes.SET_SCT_RUNNER,
            location_params={"id": str(self.run_id)},
            body={
                **self.generic_body,
                "public_ip": public_ip,
                "private_ip": private_ip,
                "region": region,
                "backend": backend,
                "name": name,
            }
        )
        self.check_response(response)

    def update_scylla_version(self, version: str) -> None:
        """
            Updates scylla server version used for filtering test results by version.
        """
        response = super().update_product_version(run_type=self.test_type, run_id=self.run_id, product_version=version)
        self.check_response(response)

    def submit_event(self, event_data: RawEventPayload | list[RawEventPayload]):
        response = self.post(
            endpoint=self.Routes.SUBMIT_EVENT,
            location_params={"id": str(self.run_id)},
            body={
                **self.generic_body,
                "data": event_data,
            }
        )
        self.check_response(response)

    def submit_sct_logs(self, logs: list[LogLink]) -> None:
        """
            Submits links to logs collected from nodes by SCT
        """
        response = super().submit_logs(run_type=self.test_type, run_id=self.run_id, logs=logs)
        self.check_response(response)

    def finalize_sct_run(self) -> None:
        """
            Marks run as finished inside argus. Currently this only sets end time of the run,
            run status must be updated separately by .set_sct_run_status
        """
        response = super().finalize_run(run_type=self.test_type, run_id=self.run_id)
        self.check_response(response)

    def submit_packages(self, packages: list[Package]):
        """
            Submits packages, which are relevant to this test - kernel versions, scylla-server versions, etc
        """
        response = self.post(
            endpoint=self.Routes.SUBMIT_PACKAGES,
            location_params={"id": str(self.run_id)},
            body={
                **self.generic_body,
                "packages": [asdict(p) for p in packages],
            }
        )
        self.check_response(response)

    def submit_screenshots(self, screenshot_links: list[str]) -> None:
        """
            Submits links to the screenshots from grafana, taken at the end of the test.
        """
        response = self.post(
            endpoint=self.Routes.SUBMIT_SCREENSHOTS,
            location_params={"id": str(self.run_id)},
            body={
                **self.generic_body,
                "screenshot_links": screenshot_links,
            }
        )
        self.check_response(response)

    def submit_gemini_results(self, gemini_data: GeminiResultsRequest) -> None:
        """
            Submits gemini results such as oracle node information and gemini command & its results
        """
        response = self.post(
            endpoint=self.Routes.SUBMIT_GEMINI_RESULTS,
            location_params={"id": str(self.run_id)},
            body={
                **self.generic_body,
                "gemini_data": gemini_data,
            }
        )
        self.check_response(response)

    def submit_performance_results(self, performance_results: PerformanceResultsRequest) -> None:
        """
            Submits results of a performance run. Things such as throughput stats, overall and per op
        """
        response = self.post(
            endpoint=self.Routes.SUBMIT_PERFORMANCE_RESULTS,
            location_params={"id": str(self.run_id)},
            body={
                **self.generic_body,
                "performance_results": performance_results,
            }
        )
        self.check_response(response)

    def create_resource(self, name: str, resource_type: str, public_ip: str, private_ip: str, instance_type: str,
                        region: str, provider: str, dc_name: str, rack_name: str, shards_amount: int, state=ResourceState.RUNNING) -> None:
        """
            Creates a cloud resource record in argus.
        """
        response = self.post(
            endpoint=self.Routes.CREATE_RESOURCE,
            location_params={"id": str(self.run_id)},
            body={
                **self.generic_body,
                "resource": {
                    "name": name,
                    "state": state,
                    "resource_type": resource_type,
                    "instance_details": {
                        "instance_type": instance_type,
                        "provider": provider,
                        "region": region,
                        "dc_name": dc_name,
                        "rack_name": rack_name,
                        "public_ip": public_ip,
                        "private_ip": private_ip,
                        "shards_amount": shards_amount,
                    }
                },
            }
        )
        self.check_response(response)

    def terminate_resource(self, name: str, reason: str) -> None:
        """
            Marks already created resource record as terminated.
        """
        response = self.post(
            endpoint=self.Routes.TERMINATE_RESOURCE,
            location_params={"id": str(self.run_id), "name": name},
            body={
                **self.generic_body,
                "reason": reason,
            }
        )
        self.check_response(response)

    def update_shards_for_resource(self, name: str, new_shards: int) -> None:
        """
            Separate methods to update "shards" of a resource, in cases
            where accurate reporting of shards is not possible during resource creation.
        """
        response = self.post(
            endpoint=self.Routes.UPDATE_SHARDS_FOR_RESOURCE,
            location_params={"id": str(self.run_id), "name": name},
            body={
                **self.generic_body,
                "shards": new_shards,
            }
        )
        self.check_response(response)

    def update_resource(self, name: str, update_data: dict[str, Any]) -> None:
        """
            Update fields of the resource.
        """
        response = self.post(
            endpoint=self.Routes.UPDATE_RESOURCE,
            location_params={"id": str(self.run_id), "name": name},
            body={
                **self.generic_body,
                "update_data": update_data,
            }
        )
        self.check_response(response)

    def submit_nemesis(self, name: str, class_name: str, start_time: int,
                       target_name: str, target_ip: str, target_shards: int) -> None:
        """
            Submits a nemesis record. Should then be finalized by
            .finalize_nemesis method on nemesis completion.
        """
        response = self.post(
            endpoint=self.Routes.SUBMIT_NEMESIS,
            location_params={"id": str(self.run_id)},
            body={
                **self.generic_body,
                "nemesis": {
                    "name": name,
                    "class_name": class_name,
                    "start_time": start_time,
                    "node_name": target_name,
                    "node_ip": target_ip,
                    "node_shards": target_shards,
                }
            }
        )
        self.check_response(response)

    def finalize_nemesis(self, name: str, start_time: int, status: str, message: str) -> None:
        """
            Finalizes an already submitted nemesis. Sets the status, nemesis message and end time.
        """
        response = self.post(
            endpoint=self.Routes.FINALIZE_NEMESIS,
            location_params={"id": str(self.run_id)},
            body={
                **self.generic_body,
                "nemesis": {
                    "name": name,
                    "start_time": start_time,
                    "status": status,
                    "message": message,
                }
            }
        )
        self.check_response(response)

    def submit_events(self, events: list[EventsInfo]) -> None:
        """
            Submits events collected by event collector once at the end of the test.
        """
        response = self.post(
            endpoint=self.Routes.SUBMIT_EVENTS,
            location_params={"id": str(self.run_id)},
            body={
                **self.generic_body,
                "events": [asdict(e) for e in events]
            }
        )
        self.check_response(response)

    def sct_heartbeat(self) -> None:
        """
            Updates a heartbeat field for an already existing test.
        """
        super().heartbeat(run_type=self.test_type, run_id=self.run_id)

    def sct_submit_junit_report(self, file_name: str, raw_content: str) -> None:
        """
            Submits a JUnit-formatted XML report to argus
        """
        response = self.post(
            endpoint=self.Routes.SUBMIT_JUNIT_REPORT,
            location_params={"id": str(self.run_id)},
            body={
                **self.generic_body,
                "file_name": file_name,
                "content": str(base64.encodebytes(bytes(raw_content, encoding="utf-8")), encoding="utf-8")
            }
        )
