import logging
from functools import cached_property
from difflib import unified_diff
from pathlib import Path

import yaml
from jinja2 import Template

import sdcm.utils.nemesis_jobs_configs as config
# To import all nemesis
from sdcm.nemesis import *


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
        "docker": "eu-west-1"
    }

    BACKEND_CONFIGS = {
        "docker": ["configurations/nemesis/additional_configs/docker_backend.yaml"]
    }

    def __init__(self, base_job: str = None, base_dir: str | Path = Path("."), backend: str = None) -> 'NemesisJobGenerator':
        self.base_dir = base_dir if isinstance(base_dir, Path) else Path(base_dir)
        self.verify_env()
        self.nemesis_class_list = self.load_nemesis_class_list()
        self.base_job = base_job if base_job else DEFAULT_JOB_NAME
        self.backend = backend if backend else DEFAULT_BACKEND
        self.config_template_name = f"template-{self.base_job}-base.yaml.j2"
        self.nemesis_config_template = "template-nemesis-config.yaml.j2"
        self.job_template_name = f"template-{self.base_job}.jenkinsfile.j2"

    @property
    def template_path(self) -> Path:
        return self.base_dir / "templates"

    @property
    def nemesis_test_config_dir(self):
        return self.base_dir / "configurations" / "nemesis"

    @property
    def nemesis_class_list_path(self) -> Path:
        return self.base_dir / "data_dir" / "nemesis_classes.yml"

    @property
    def base_nemesis_job_dir(self) -> Path:
        return self.base_dir / "jenkins-pipelines" / "oss" / "nemesis"

    @cached_property
    def nemesis_base_config_file_template(self) -> str:
        with (self.template_path / TEST_CASE_TEMPLATE_DIR / self.config_template_name).open() as file:
            return file.read()

    @cached_property
    def nemesis_job_template(self) -> str:
        with (self.template_path / JOB_PIPELINE_TEMPLATE_DIR / self.job_template_name).open() as file:
            return file.read()

    @cached_property
    def nemesis_config_template_content(self) -> str:
        with (self.template_path / TEST_CASE_TEMPLATE_DIR / self.nemesis_config_template).open() as file:
            return file.read()

    def load_nemesis_class_list(self) -> list[str]:
        with self.nemesis_class_list_path.open() as file:
            data = yaml.safe_load(file)

        assert isinstance(data, list), "Malformed nemesis class list file"
        assert all(isinstance(cl, str) for cl in data), "Nemesis class list contains non-string values"

        return data

    def create_test_cases_from_template(self):
        for cls in self.nemesis_class_list:
            new_config_name = f"{cls}.yaml"
            nemesis_config_body = Template(self.nemesis_config_template_content).render(
                {"nemesis_class": cls})
            target_config = self.nemesis_test_config_dir / new_config_name
            if target_config.exists():
                handle = target_config.open("r+")
                content = handle.read()
                if len(content) != len(nemesis_config_body):
                    LOGGER.warning("Potential altered config %s.yaml, checking for conflicts", cls)
                    new_config = yaml.safe_load(nemesis_config_body)
                    old_config = yaml.safe_load(content)
                    new_keys = set(new_config.keys())
                    old_keys = set(old_config.keys())
                    if len((key_diff := new_keys.symmetric_difference(old_keys))) != 0:
                        diff = unified_diff(
                            content.splitlines(keepends=True),
                            nemesis_config_body.splitlines(keepends=True),
                            fromfile=f"{cls}-old.yaml",
                            tofile=f"{cls}-new.yaml"
                        )
                        LOGGER.error("Difference detected in %s.yaml, Diff:\n%s\n\nExtra keys: %s",
                                     cls, "\n".join(diff), ", ".join(key_diff))
                        continue
            else:
                handle = target_config.open("w")
            handle.seek(0)
            handle.truncate()
            handle.write(nemesis_config_body)
            handle.write("\n")
            handle.close()
            LOGGER.info("Created test config file: %s", new_config_name)

    def create_job_files_from_template(self) -> list[Path]:
        pipeline_files = []
        backend_config = config.NEMESIS_REQUIRED_ADDITIONAL_CONFIGS.get(self.backend, [])
        for cls in self.nemesis_class_list:
            additional_configs = config.NEMESIS_REQUIRED_ADDITIONAL_CONFIGS.get(cls, [])
            additional_params = config.NEMESIS_ADDITIONAL_PIPELINE_PARAMS.get(cls, {})
            config_name = [
                str(self.nemesis_test_config_dir / f"{self.base_job}-nemesis.yaml"),
                str(self.nemesis_test_config_dir / f"{cls}.yaml"),
                *backend_config,
                *additional_configs,
            ]
            job_file_name = f"{self.base_job}-{cls}-{self.backend}.jenkinsfile"
            nemesis_job_groovy_source = Template(self.nemesis_job_template).render(
                {
                    "params": {
                        "backend": self.backend,
                        "region": self.BACKEND_TO_REGION.get(self.backend, "eu-west-1"),
                        "test_name": 'longevity_test.LongevityTest.test_custom_time',
                        "test_config": config_name,
                        **additional_params,
                    }

                })
            job_path = self.base_nemesis_job_dir / job_file_name
            with job_path.open("w") as file:
                file.write(nemesis_job_groovy_source + "\n")
            LOGGER.info("Created pipeline file: %s", job_file_name)

            pipeline_files.append(job_path)

        return pipeline_files

    def render_base_job_config(self, params: dict | None = None):
        params = params if params else {}
        base_config_body = Template(self.nemesis_base_config_file_template).render(params)
        with (self.nemesis_test_config_dir / f"{self.base_job}-nemesis.yaml").open(mode="w") as file:
            file.write(base_config_body + '\n')

        LOGGER.info("Rendered base config:\n\n%s\n\n", self.nemesis_base_config_file_template)

    def verify_env(self):
        assert self.template_path.exists(), \
            "Nemesis template directory is missing, cannot continue"
        assert self.nemesis_class_list_path.exists(), \
            "Nemesis class list is not generated, cannot continue"

        if not self.nemesis_test_config_dir.exists():
            self.nemesis_test_config_dir.mkdir()

        if not self.base_nemesis_job_dir.exists():
            self.base_nemesis_job_dir.mkdir()
