import os

from sdcm import sct_abs_path


JAVA_DOCKER_IMAGE = 'eclipse-temurin:25-jre-alpine'


class JavaContainerMixin:
    def java_container_run_args(self) -> dict:
        user_home = os.path.expanduser("~")
        volumes = {
            user_home: {"bind": user_home, "mode": "rw"},
            sct_abs_path(""): {"bind": sct_abs_path(""), "mode": "ro"},
            '/tmp': {"bind": "/tmp", "mode": "rw"},
        }
        return dict(image=JAVA_DOCKER_IMAGE,
                    entrypoint="cat",
                    tty=True,
                    name=f"{self.name}-java",
                    network_mode="host",
                    user=f"{os.getuid()}:{os.getgid()}",
                    volumes=volumes,
                    environment={},
                    )
