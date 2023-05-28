import json
import logging
import random
import traceback
import yaml

from sdcm.sct_events import Severity
from sdcm.sct_events.system import PerftuneResultEvent


# https://docs.scylladb.com/stable/operating-scylla/admin-tools/perftune.html


PERFTUNE_LOCATION = "/opt/scylladb/scripts/perftune.py"
TEMP_PERFTUNE_YAML_PATH = "/tmp/perftune.yaml"
PERFTUNE_EXPECTED_RESULTS_PATH = "defaults/perftune_results.json"


def get_machine_architecture_type(node):
    result = node.remoter.run("uname -m")
    return result.stdout.strip()


def get_number_of_cpu_cores(node) -> int:
    result = node.remoter.run("grep -c ^processor /proc/cpuinfo")
    cores_num = int(result.stdout)
    return cores_num


def get_default_mode(node):
    number_of_cores = get_number_of_cpu_cores(node=node)
    if number_of_cores <= 4:
        return "mq"
    elif number_of_cores <= 8:
        return "sq"
    elif number_of_cores <= 32:
        return "sq_split"
    return None


class PerftuneExpectedResult:
    def __init__(self, cluster_backend, number_of_cpu_cores, nic_name, architecture="x86_64"):
        self.nic_name = nic_name
        with open(PERFTUNE_EXPECTED_RESULTS_PATH, encoding="utf-8") as expected_results_file:
            expected_results_dict_all_instances = json.loads(expected_results_file.read())
        self.expected_results_for_instance = \
            expected_results_dict_all_instances[cluster_backend][architecture][str(number_of_cpu_cores)]

    def get_expected_cpu_mask(self) -> str:
        return self.expected_results_for_instance.get("get-cpu-mask")

    def get_expected_irq_cpu_mask(self) -> str:  # Only if the command available:
        return self.expected_results_for_instance.get("get-irq-cpu-mask")

    def get_expected_options_file_contents(self) -> dict:
        base_result_dict = self.expected_results_for_instance.get("dump-options-file")
        attach_values = {
            "nic": [self.nic_name],
            "irq_core_auto_detection_ratio": 16,
            "tune": ["net"]
        }
        base_result_dict.update(attach_values)
        return base_result_dict


class PerftuneExecutor:
    def __init__(self, node, nic_name):
        self.node = node
        self.nic_name = nic_name

    def get_cpu_mask(self) -> str:
        result = self.node.remoter.run(f"{PERFTUNE_LOCATION} --tune net --nic {self.nic_name} --get-cpu-mask")
        return result.stdout.strip()

    def get_irq_cpu_mask(self) -> str:
        result = self.node.remoter.run(f"{PERFTUNE_LOCATION} --tune net --nic {self.nic_name} --get-irq-cpu-mask")
        return result.stdout.strip()

    def get_options_file_contents(self, use_temp_file=False, mode="", override_irq_cpu_mask="") -> dict:
        cmd = f"{PERFTUNE_LOCATION} --tune net --nic {self.nic_name} --dump-options-file"
        if mode:
            cmd += f" --mode {mode}"
        if use_temp_file:
            cmd += f" --options-file {TEMP_PERFTUNE_YAML_PATH}"
        if override_irq_cpu_mask:
            cmd += f" --irq-cpu-mask {override_irq_cpu_mask}"
        result = self.node.remoter.run(cmd)
        result_as_yaml = yaml.safe_load(result.stdout)
        return result_as_yaml

    def create_temp_perftune_yaml(self, yaml_dict) -> None:
        self.node.remoter.run(f"touch {TEMP_PERFTUNE_YAML_PATH}")
        with self.node._remote_yaml(path=TEMP_PERFTUNE_YAML_PATH, sudo=False) as temp_yaml:  # pylint: disable=protected-access
            temp_yaml.update(yaml_dict)


class PerftuneOutputChecker:  # pylint: disable=too-few-public-methods
    def __init__(self, node, cluster_backend):
        self.log = logging.getLogger(self.__class__.__name__)
        self.node = node
        nic_name = node.get_nic_devices()[0]
        self.executor = PerftuneExecutor(node, nic_name)
        self.expected_result = PerftuneExpectedResult(cluster_backend, get_number_of_cpu_cores(node), nic_name,
                                                      architecture=get_machine_architecture_type(node))

    def compare_cpu_mask(self) -> None:
        cpu_mask = self.executor.get_cpu_mask()
        if cpu_mask != self.expected_result.get_expected_cpu_mask():
            PerftuneResultEvent(
                message=f"Mismatched results when testing the output of the 'get-cpu-mask' command on {self.node}"
                        f"\nActual result: '{cpu_mask}'"
                        f"\nExpected output: '{self.expected_result.get_expected_cpu_mask()}'",
                severity=Severity.ERROR).publish()

    def compare_irq_cpu_mask(self) -> None:
        irq_cpu_mask = self.executor.get_irq_cpu_mask()
        if irq_cpu_mask != self.expected_result.get_expected_irq_cpu_mask():
            PerftuneResultEvent(
                message=f"Mismatched results when testing the output of the 'get-irq-cpu-mask' command on "
                        f"{self.node}"
                        f"\nActual result: '{irq_cpu_mask}'"
                        f"\nExpected output: '{self.expected_result.get_expected_irq_cpu_mask()}'",
                severity=Severity.ERROR).publish()

    def compare_default_option_file(self, option_file_dict) -> None:
        if option_file_dict != self.expected_result.get_expected_options_file_contents():
            PerftuneResultEvent(
                message=f"Mismatched results when testing the output of the 'dump-options-file' command on "
                        f"{self.node}"
                        f"\nActual result: '{option_file_dict}'"
                        f"\nExpected output: '{self.expected_result.get_expected_options_file_contents()}'",
                severity=Severity.ERROR).publish()

    def compare_option_file_yaml_with_temp_yaml_copy(self, option_file_dict) -> None:
        temp_perftune_yaml_content_dict = self.executor.get_options_file_contents(use_temp_file=True)
        if temp_perftune_yaml_content_dict != option_file_dict:
            PerftuneResultEvent(
                message=f"Mismatched results when comparing the output of the 'dump-options-file' command to "
                        f"the content of the generated perftune.yaml file on {self.node}"
                        f"\nActual result: '{temp_perftune_yaml_content_dict}'"
                        f"\nExpected output: '{option_file_dict}'",
                severity=Severity.ERROR).publish()

    @staticmethod
    def _generate_new_irq_cpu_mask_string(mask_int_values) -> str:
        new_masks = []
        for mask_value in mask_int_values:
            random_irq_int_value = random.randint(0, mask_value)
            # The irq_cpu_mask cannot be greater than the cpu_mask, since it is a part of it
            new_mask_string = "{0:x}".format(random_irq_int_value)  # converting back to base 16
            padded_new_mask_string = f"0x{new_mask_string.rjust(8, '0')}"  # Padding
            new_masks.append(padded_new_mask_string)
        return ",".join(new_masks)

    @staticmethod
    def get_mask_int_values(complete_mask_string) -> list:
        split_masks = complete_mask_string.split(",")
        numerical_values = []
        for mask_string in split_masks:
            num_value = int(mask_string, base=16)
            numerical_values.append(num_value)
        return numerical_values

    def compare_option_file_with_overridden_irq_cpu_mask_param(self, option_file_dict) -> None:
        current_cpu_mask = option_file_dict["cpu_mask"]
        cpu_mask_int_values = self.get_mask_int_values(current_cpu_mask)
        random_irq_cpu_mask_string = self._generate_new_irq_cpu_mask_string(cpu_mask_int_values)
        altered_option_file_contents = self.executor.get_options_file_contents(
            override_irq_cpu_mask=random_irq_cpu_mask_string)
        if altered_option_file_contents["irq_cpu_mask"] != random_irq_cpu_mask_string:
            PerftuneResultEvent(
                message=f"Despite overriding the irq_cpu_mask param, its value is was not altered properly in the "
                        f"output of the 'dump-options-file' command on node {self.node}",
                severity=Severity.ERROR).publish()

    def compare_perftune_results(self) -> None:
        PerftuneResultEvent(
            message="Checking the output of perftune.py",
            severity=Severity.NORMAL).publish()
        try:
            self.compare_cpu_mask()
            self.compare_irq_cpu_mask()
            default_mode = get_default_mode(self.node)
            override_irq_cpu_mask = None
            if not default_mode:
                override_irq_cpu_mask = self.expected_result.get_expected_irq_cpu_mask()
            option_file_dict = self.executor.get_options_file_contents(mode=default_mode,
                                                                       override_irq_cpu_mask=override_irq_cpu_mask)
            self.compare_default_option_file(option_file_dict)
            self.executor.create_temp_perftune_yaml(yaml_dict=option_file_dict)
            self.compare_option_file_yaml_with_temp_yaml_copy(option_file_dict)
            self.compare_option_file_with_overridden_irq_cpu_mask_param(option_file_dict)
        except Exception as error:  # pylint: disable=broad-except  # noqa: BLE001
            PerftuneResultEvent(
                message=f"Unexpected error when verifying the output of Perftune: {error}",
                severity=Severity.ERROR).publish()
            self.log.error(traceback.format_exc())
