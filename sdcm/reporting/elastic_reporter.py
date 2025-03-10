import datetime
import logging


from sdcm.es import ES
from sdcm.utils.ci_tools import get_job_name, get_job_url

LOGGER = logging.getLogger(__name__)


class ElasticRunReporter:

    INDEX_NAME = "sct_test_runs"

    def __init__(self) -> None:
        self._es = ES()

    @staticmethod
    def get_build_number(build_job_url: str) -> int | None:
        build_number = build_job_url.rstrip("/").split("/")[-1] if build_job_url else -1
        if build_number:
            try:
                return int(build_number)
            except ValueError:
                LOGGER.error("Error parsing build number from %s: got %s as build_number", build_job_url, build_number)
        return None

    def report_run(self, run_id: str, status: str, index=None) -> bool:
        job_name = get_job_name()
        if job_name == "local_run":
            LOGGER.warning("Will not report a local run to elastic, aborting.")
            return False
        build_url = get_job_url()
        if not build_url:
            LOGGER.warning("Build URL is missing, unable to report the run.")
            return False
        build_number = self.get_build_number(build_url)

        index = self.INDEX_NAME if not index else index
        if not self._check_index(index):
            self._es.indices.create(index=self.INDEX_NAME)
        document = {
            "timestamp": datetime.datetime.utcnow(),
            "run_id": run_id,
            "status": status,
            "build_id": job_name,
            "build_url": build_url,
            "argus_url": f"https://argus.scylladb.com/tests/scylla-cluster-tests/{run_id}",
            "build_number": build_number,
        }

        self._es.create(index=index, document=document, id=run_id)
        return True

    def _check_index(self, index_name: str):
        return self._es.indices.exists(index=index_name)
