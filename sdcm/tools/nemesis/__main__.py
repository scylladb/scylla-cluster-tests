from dataclasses import field, dataclass

import click
import yaml

from sdcm import sct_abs_path
from sdcm.nemesis import SisyphusMonkey
from sdcm.tools.nemesis.generate_nemesis_jobs import NemesisJobGenerator
from sdcm.tools.nemesis.verify import Verify, Fix

PARAMS = dict(nemesis_interval=1, nemesis_filter_seeds=False)


@dataclass
class Cluster:
    nodes: list = field(default_factory=list)


@dataclass
class FakeTester:
    params: dict = field(default_factory=lambda: PARAMS)
    loaders: list = field(default_factory=list)
    db_cluster: dict = field(default_factory=lambda: Cluster())
    monitors: list = field(default_factory=list)

    def __post_init__(self):
        self.db_cluster.params = self.params


def create_yaml(sisiphus_monkey: SisyphusMonkey, file_opener=open):
    _, disruptions_dict, disruption_classes = sisiphus_monkey.get_list_of_disrupt_methods(
        subclasses_list=sisiphus_monkey._get_subclasses(), export_properties=True)

    with file_opener(sct_abs_path('data_dir/nemesis.yml'), 'w', encoding="utf-8") as outfile1:
        yaml.dump(disruptions_dict, outfile1, default_flow_style=False)

    with file_opener(sct_abs_path('data_dir/nemesis_classes.yml'), 'w', encoding="utf-8") as outfile2:
        yaml.dump(disruption_classes, outfile2, default_flow_style=False)


def _generate_yaml(verify: bool, fix: bool):
    if verify:
        create_yaml(SisyphusMonkey(FakeTester(), None), Fix if fix else Verify)
        if Verify.ERROR or Fix.ERROR:
            return 1
    else:
        create_yaml(SisyphusMonkey(FakeTester(), None))
    return 0


def _generate_pipeline(verify: bool, fix: bool):
    if verify:
        generator = NemesisJobGenerator(Fix if fix else Verify, base_dir="")
    else:
        generator = NemesisJobGenerator(open, base_dir="")
    generator.render_base_job_config()
    generator.create_test_cases_from_template()
    generator.create_job_files_from_template()

    if Verify.ERROR or Fix.ERROR:
        return 1
    return 0


@click.group()
def cli():
    pass


@cli.command()
@click.option('--verify', is_flag=True, help='True, if the command should only compare with existing files')
@click.option('--fix', is_flag=True, help='True, if the command should also fix the issues it found with verify')
def generate_yaml(verify: bool, fix: bool):
    return _generate_yaml(verify, fix)


@cli.command()
@click.option('--verify', is_flag=True, help='True, if the command should only compare with existing files')
@click.option('--fix', is_flag=True, help='True, if the command should also fix the issues it found with verify')
def generate_pipeline(verify: bool, fix: bool):
    return _generate_pipeline(verify, fix)


@cli.command()
@click.option('--verify', is_flag=True, help='True, if the command should only compare with existing files')
@click.option('--fix', is_flag=True, help='True, if the command should also fix the issues it found with verify')
def generate_all(verify: bool, fix: bool):
    return_code = _generate_yaml(verify, fix)
    return_code += _generate_pipeline(verify, fix)
    return return_code


if __name__ == '__main__':
    cli()
