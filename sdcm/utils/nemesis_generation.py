from pathlib import Path

import yaml
from jinja2 import Template

from sdcm import sct_abs_path
from sdcm.nemesis import *

DEFAULT_JOB_NAME = "longevity-5gb-1h"
TEST_CASE_TEMPLATE_DIR = "test_config"
JOB_PIPELINE_TEMPLATE_DIR = "job_pipeline"
DEFAULT_BACKEND = "aws"
LOGGER = logging.getLogger(__name__)


def generate_nemesis_yaml(file_opener=open):
    """Generates both nemesis.yaml and nemesis_classes.yml"""
    registry = NemesisRegistry(Nemesis, NemesisFlags, COMPLEX_NEMESIS)
    class_properties, method_properties = registry.gather_properties()
    sorted_dict = dict(sorted(method_properties.items(), key=lambda d: d[0]))
    with file_opener(sct_abs_path('data_dir/nemesis.yml'), 'w', encoding="utf-8") as outfile1:
        yaml.dump(sorted_dict, outfile1, default_flow_style=False)

    with file_opener(sct_abs_path('data_dir/nemesis_classes.yml'), 'w', encoding="utf-8") as outfile2:
        yaml.dump(sorted(class_properties.keys()), outfile2, default_flow_style=False)


class NemesisJobGenerator:
    """Generates Config files and pipelines for all nemesis"""
    BACKEND_TO_REGION = {
        "aws": "eu-west-1",
        "gce": "us-east1",
        "azure": "eastus",
        "docker": "eu-west-1"
    }

    BACKEND_CONFIGS = {
        "docker": ["configurations/nemesis/additional_configs/docker_backend.yaml"]
    }

    def __init__(self, file_opener=open, base_job: str = None, base_dir: str | Path = Path("../.."), backends: list[str] = None):
        self.file_opener = file_opener
        self.base_dir = base_dir if isinstance(base_dir, Path) else Path(base_dir)
        self.verify_env()
        self.nemesis_class_list = self.load_nemesis_class_list()
        self.base_job = base_job if base_job else DEFAULT_JOB_NAME
        self.backends = backends if backends else self.BACKEND_TO_REGION.keys()
        self.config_template_name = f"template-{self.base_job}-base.yaml.j2"
        self.nemesis_config_template = "template-nemesis-config.yaml.j2"
        self.job_template_name = f"template-{self.base_job}.jenkinsfile.j2"

        for backend in self.backends:
            if backend not in self.BACKEND_TO_REGION:
                raise ValueError(f"Invalid backend: {backend}")

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
            with self.file_opener(target_config, "w") as file:
                file.write(nemesis_config_body)
                file.write("\n")

    def create_job_files_from_template(self):
        for cls in self.nemesis_class_list:
            clazz = globals()[cls]
            additional_configs = clazz.additional_configs or []
            additional_params = clazz.additional_params or {}
            for backend in self.backends:
                backend_config = self.BACKEND_CONFIGS.get(backend, [])
                config_name = [
                    str(self.nemesis_test_config_dir / f"{self.base_job}-nemesis.yaml"),
                    str(self.nemesis_test_config_dir / f"{cls}.yaml"),
                    *backend_config,
                    *additional_configs,
                ]
                job_file_name = f"{self.base_job}-{cls}-{backend}.jenkinsfile"
                nemesis_job_groovy_source = Template(self.nemesis_job_template).render(
                    {
                        "params": {
                            "backend": backend,
                            "region": self.BACKEND_TO_REGION.get(backend, "eu-west-1"),
                            "test_name": 'longevity_test.LongevityTest.test_custom_time',
                            "test_config": config_name,
                            **additional_params,
                        }

                    })
                job_path = self.base_nemesis_job_dir / job_file_name
                with self.file_opener(job_path, "w") as file:
                    file.write(nemesis_job_groovy_source + "\n")

    def render_base_job_config(self, params: dict | None = None):
        params = params if params else {}
        base_config_body = Template(self.nemesis_base_config_file_template).render(params)
        with self.file_opener(self.nemesis_test_config_dir / f"{self.base_job}-nemesis.yaml", "w") as file:
            file.write(base_config_body + '\n')

    def verify_env(self):
        assert self.template_path.exists(), \
            "Nemesis template directory is missing, cannot continue"
        assert self.nemesis_class_list_path.exists(), \
            "Nemesis class list is not generated, cannot continue"

        if not self.nemesis_test_config_dir.exists():
            self.nemesis_test_config_dir.mkdir()

        if not self.base_nemesis_job_dir.exists():
            self.base_nemesis_job_dir.mkdir()
