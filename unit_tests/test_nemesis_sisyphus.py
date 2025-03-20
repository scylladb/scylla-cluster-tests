import yaml

from sdcm import sct_abs_path
from sdcm.nemesis import Nemesis, COMPLEX_NEMESIS
from sdcm.nemesis_registry import NemesisRegistry


def test_list_all_available_nemesis(generate_file=True):
    registry = NemesisRegistry(Nemesis, COMPLEX_NEMESIS)
    disruption_list = registry.get_disrupt_methods()

    assert len(disruption_list) == 92

    class_properties, method_properties = registry.gather_properties()
    sorted_dict = dict(sorted(method_properties.items(), key=lambda d: d[0]))
    if generate_file:
        with open(sct_abs_path('data_dir/nemesis.yml'), 'w', encoding="utf-8") as outfile1:
            yaml.dump(sorted_dict, outfile1, default_flow_style=False)

        with open(sct_abs_path('data_dir/nemesis_classes.yml'), 'w', encoding="utf-8") as outfile2:
            yaml.dump(sorted(class_properties.keys()), outfile2, default_flow_style=False)

    with open(sct_abs_path('data_dir/nemesis.yml'), 'r', encoding="utf-8") as nemesis_file:
        static_nemesis_list = yaml.safe_load(nemesis_file)

    assert static_nemesis_list == method_properties
