import logging
from functools import cached_property
from pathlib import Path

import yaml
from jinja2 import Template


DEFAULT_JOB_NAME = "longevity-5gb-1h"
TEST_CASE_TEMPLATE_DIR = "test_config"
JOB_PIPELINE_TEMPLATE_DIR = "job_pipeline"
DEFAULT_BACKEND = "aws"
LOGGER = logging.getLogger(__name__)


class NemesisJobGenerator:
    BACKEND_TO_REGION = {
        "aws": "eu-west-1",
        "gce": "us-east1",
        "azure": "eastus",
    }

    def __init__(self, base_job: str = None, base_dir: str | Path = Path("."), backend: str = None) -> 'NemesisJobGenerator':
        self.base_dir = base_dir if isinstance(base_dir, Path) else Path(base_dir)
        self.verify_env()
        self.nemesis_class_list = self.load_nemesis_class_list()
        self.base_job = base_job if base_job else DEFAULT_JOB_NAME
        self.backend = backend if backend else DEFAULT_BACKEND
        self.config_template_name = f"template-{self.base_job}.yaml.j2"
        self.job_template_name = f"template-{self.base_job}.jenkinsfile.j2"

    @property
    def template_path(self) -> Path:
        return self.base_dir / "templates"

    @property
    def nemesis_test_config_dir(self):
        return self.base_dir / "test-cases" / "nemesis"

    @property
    def nemesis_class_list_path(self) -> Path:
        return self.base_dir / "data_dir" / "nemesis_classes.yml"

    @property
    def base_nemesis_job_dir(self) -> Path:
        return self.base_dir / "jenkins-pipelines" / "nemesis"

    @cached_property
    def nemesis_config_file_template(self) -> str:
        with (self.template_path / TEST_CASE_TEMPLATE_DIR / self.config_template_name).open() as file:
            return file.read()

    @cached_property
    def nemesis_job_template(self) -> str:
        with (self.template_path / JOB_PIPELINE_TEMPLATE_DIR / self.job_template_name).open() as file:
            return file.read()

    def load_nemesis_class_list(self) -> list[str]:
        with self.nemesis_class_list_path.open() as file:
            data = yaml.safe_load(file)

        assert isinstance(data, list), "Malformed nemesis class list file"
        assert all(isinstance(cl, str) for cl in data), "Nemesis class list contains non-string values"

        return data

    def create_test_cases_from_template(self):
        for cls in self.nemesis_class_list:
            new_config_name = f"{self.base_job}-{cls}.yaml"
            nemesis_config_body = Template(self.nemesis_config_file_template).render(
                {"nemesis_class": cls})
            with (self.nemesis_test_config_dir / new_config_name).open(mode="w") as file:
                file.write(nemesis_config_body)
            LOGGER.info("Created test config file: %s", new_config_name)

    def create_job_files_from_template(self) -> list[Path]:
        pipeline_files = []
        for cls in self.nemesis_class_list:
            config_name = f"{self.base_job}-{cls}.yaml"
            job_file_name = f"{self.base_job}-{cls}-{self.backend}.jenkinsfile"
            nemesis_job_groovy_source = Template(self.nemesis_job_template).render(
                {
                    "nemesis_longevity_config": config_name,
                    "backend": self.backend,
                    "region": self.BACKEND_TO_REGION.get(self.backend, "eu-west-1")
                })
            job_path = self.base_nemesis_job_dir / job_file_name
            with job_path.open("w") as file:
                file.write(nemesis_job_groovy_source)
            LOGGER.info("Created pipeline file: %s", job_file_name)

            pipeline_files.append(job_path)

        return pipeline_files

    def verify_env(self):
        assert self.template_path.exists(), \
            "Nemesis template directory is missing, cannot continue"
        assert self.nemesis_class_list_path.exists(), \
            "Nemesis class list is not generated, cannot continue"

        if not self.nemesis_test_config_dir.exists():
            self.nemesis_test_config_dir.mkdir()

        if not self.base_nemesis_job_dir.exists():
            self.base_nemesis_job_dir.mkdir()
