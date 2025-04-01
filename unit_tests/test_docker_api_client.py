import logging
import pytest
from pathlib import Path


from sdcm.utils.docker_api_client import DockerAPIClient, DockerCmdExecutor, DockerCmdResult

LOGGER = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.usefixtures("events"),
]


def test_docker_cmd_result():
    result = DockerCmdResult(
        exit_code=0, stdout="test output", stderr="test error", command="test command")
    assert result.ok
    assert result.stdout == "test output"
    assert result.stderr == "test error"
    assert result.command == "test command"
    assert "Command: test command" in str(result)


@pytest.mark.docker_scylla_args(ssl=False)
class TestDockerAPIClient:

    @pytest.fixture(scope="function")
    def docker_client(self):
        client = DockerAPIClient()
        yield client
        client.client.close()

    @pytest.fixture(scope="function")
    def container_id(self, docker_scylla):
        return docker_scylla.docker_id

    def test_get_container(self, container_id, docker_client):
        container = docker_client.get_container(container_id)
        assert container.id == container_id
        assert docker_client.get_container("non_existent_container") is None

    def test_execute_command(self, container_id, docker_client):
        exit_code, stdout, stderr = docker_client.execute_command(container_id, "echo 'test'")
        assert exit_code == 0
        assert "test" in stdout
        assert not stderr

        exit_code, stdout, stderr = docker_client.execute_command(container_id, "non_existent_command")
        assert exit_code != 0

    def test_copy_file_operations(self, container_id, docker_client, tmp_path):
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")
        docker_client.copy_to_container(container_id, test_file, Path("/tmp/test.txt"))
        exit_code, stdout, _ = docker_client.execute_command(container_id, "cat /tmp/test.txt")
        assert exit_code == 0
        assert "test content" in stdout

        output_file = tmp_path / "output.txt"
        docker_client.copy_from_container(container_id, Path("/tmp/test.txt"), output_file)
        assert output_file.read_text() == "test content"


@pytest.mark.docker_scylla_args(ssl=False)
class TestDockerCmdExecutor:

    @pytest.fixture(scope="function")
    def cmd_executor(self):
        return DockerCmdExecutor()

    @pytest.fixture(scope="function")
    def container_id(self, docker_scylla):
        return docker_scylla.docker_id

    def test_run_command(self, container_id, cmd_executor):
        result = cmd_executor.run(container_id, "echo 'test'")
        assert result.ok
        assert "test" in result.stdout

        result = cmd_executor.run(container_id, "non_existent_command")
        assert result.exit_code != 0

        result = cmd_executor.run("bad_container_id", "echo 'test'", ignore_status=True)
        assert not result.ok
        assert result.exit_code != 0

    def test_sudo_command(self, container_id, cmd_executor):
        result = cmd_executor.sudo(container_id, "echo 'test sudo'")
        assert result.ok and "test sudo" in result.stdout

    def test_is_up(self, container_id, cmd_executor):
        assert cmd_executor.is_up(container_id)
        assert not cmd_executor.is_up("non_existent_container")

    def test_command_timeout(self, container_id, cmd_executor):
        with pytest.raises(TimeoutError):
            cmd_executor.run(container_id, "sleep 10", timeout=2)

    def test_retry_mechanism(self, container_id, cmd_executor, monkeypatch):
        attempt_count = 0

        def counting_execute(*args, **kwargs):
            nonlocal attempt_count
            attempt_count += 1
            raise Exception("Command not found")
        monkeypatch.setattr(cmd_executor.docker_client, 'execute_command', counting_execute)

        with pytest.raises(Exception):
            cmd_executor.run(container_id, "non_existent_command", retry=2, interval=1)
        assert attempt_count == 3, f"Expected 3 attempts (1 original + 2 retries), got {attempt_count}"
