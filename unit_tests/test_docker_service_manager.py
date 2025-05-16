import pytest
from sdcm.utils.docker_service_manager import create_service_manager

pytestmark = [
    pytest.mark.integration,
    pytest.mark.usefixtures("events"),
]


@pytest.mark.docker_scylla_args(ssl=False)
class TestDockerServiceManager:

    @pytest.fixture(scope="function")
    def container_id(self, docker_scylla):
        return docker_scylla.docker_id

    @pytest.fixture(scope="function")
    def service_manager(self, container_id):
        return create_service_manager(container_id)

    def test_service_status(self, service_manager):
        assert service_manager.get_service_status("scylla", user="root") in ["running", "active"]
        assert service_manager.get_service_status("non_existent_service") == "unknown"

    def _test_stop_start_service(self, service_manager):
        service_name = "cron"
        assert service_manager.stop_service(service_name)
        assert not service_manager.is_service_running(service_name)

        assert service_manager.start_service(service_name)
        assert service_manager.is_service_running(service_name)

    def _test_restart_service(self, service_manager):
        service_name = "cron"
        assert service_manager.is_service_running(service_name)
        assert service_manager.restart_service(service_name)
        assert service_manager.is_service_running(service_name)
