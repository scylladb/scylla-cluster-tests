import yaml

from sdcm import sct_abs_path
from sdcm.nemesis import Nemesis, COMPLEX_NEMESIS, DEPRECATED_LIST_OF_NEMESISES
from sdcm.nemesis_registry import NemesisRegistry


def test_list_all_available_nemesis(generate_file=True):
    sisyphus = NemesisRegistry(Nemesis, False, COMPLEX_NEMESIS + DEPRECATED_LIST_OF_NEMESISES)

    subclasses = sisyphus.get_subclasses()
    disruption_list, disruptions_dict, disruption_classes = sisyphus.get_list_of_disrupt_methods(
        subclasses_list=subclasses, export_properties=True)

    assert len(disruption_list) == 92

    if generate_file:
        with open(sct_abs_path('data_dir/nemesis.yml'), 'w', encoding="utf-8") as outfile1:
            yaml.dump(disruptions_dict, outfile1, default_flow_style=False)

        with open(sct_abs_path('data_dir/nemesis_classes.yml'), 'w', encoding="utf-8") as outfile2:
            yaml.dump(disruption_classes, outfile2, default_flow_style=False)

    with open(sct_abs_path('data_dir/nemesis.yml'), 'r', encoding="utf-8") as nemesis_file:
        static_nemesis_list = yaml.safe_load(nemesis_file)

    assert static_nemesis_list == disruptions_dict
