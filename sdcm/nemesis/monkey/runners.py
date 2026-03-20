"""
NemesisRunner override, also known as Complex Nemesis
Runners can be used in nemesis_class_names config option
"""

import random
import time
from typing import Callable, List, Set, Tuple

from sdcm.mgmt.common import ObjectStorageUploadMode
from sdcm.nemesis import NemesisRunner


class SisyphusMonkey(NemesisRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.disruptions_list = self.build_disruptions_by_selector(self.nemesis_selector)
        self.disruptions_list = self.shuffle_list_of_disruptions(self.disruptions_list)


class NoOpMonkey(NemesisRunner):
    kubernetes = True

    def call_next_nemesis(self):
        time.sleep(300)


class ScyllaCloudLimitedChaosMonkey(NemesisRunner):
    # Limit the nemesis scope to only one relevant to scylla cloud, where we defined we don't have AWS api access:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.disruptions_list = self.build_disruptions_by_name(
            [
                "NodeToolCleanupMonkey",
                "DrainerMonkey",
                "RefreshMonkey",
                "StopStartMonkey",
                "MajorCompactionMonkey",
                "ModifyTableMonkey",
                "EnospcMonkey",
                "StopWaitStartMonkey",
                "SoftRebootNodeMonkey",
                "TruncateMonkey",
            ]
        )
        self.disruptions_list = self.shuffle_list_of_disruptions(self.disruptions_list)


class DisruptKubernetesNodeThenReplaceScyllaNode(NemesisRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.disruptions_list = self.build_disruptions_by_name(
            [
                "DrainKubernetesNodeThenReplaceScyllaNode",
                "TerminateKubernetesHostThenReplaceScyllaNode",
            ]
        )
        self.disruptions_list = self.shuffle_list_of_disruptions(self.disruptions_list)


class DisruptKubernetesNodeThenDecommissionAndAddScyllaNode(NemesisRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.disruptions_list = self.build_disruptions_by_name(
            [
                "DrainKubernetesNodeThenDecommissionAndAddScyllaNode",
                "TerminateKubernetesHostThenDecommissionAndAddScyllaNode",
            ]
        )
        self.disruptions_list = self.shuffle_list_of_disruptions(self.disruptions_list)


class K8sSetMonkey(NemesisRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.disruptions_list = self.build_disruptions_by_name(
            [
                "DrainKubernetesNodeThenReplaceScyllaNode",
                "TerminateKubernetesHostThenReplaceScyllaNode",
                "DrainKubernetesNodeThenDecommissionAndAddScyllaNode",
                "TerminateKubernetesHostThenDecommissionAndAddScyllaNode",
            ]
        )
        self.disruptions_list = self.shuffle_list_of_disruptions(self.disruptions_list)


class ScyllaOperatorBasicOperationsMonkey(NemesisRunner):
    """
    Selected number of nemesis that is focused on scylla-operator functionality
    """

    disruptive = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.disruptions_list = self.build_disruptions_by_name(
            [
                "OperatorNodetoolFlushAndReshard",
                "ClusterRollingRestartRandomOrder",
                "GrowShrinkClusterNemesis",
                "AddRemoveRackNemesis",
                "StopStartMonkey",
                "DrainKubernetesNodeThenReplaceScyllaNode",
                "TerminateKubernetesHostThenReplaceScyllaNode",
                "DrainKubernetesNodeThenDecommissionAndAddScyllaNode",
                "TerminateKubernetesHostThenDecommissionAndAddScyllaNode",
                "OperatorNodeReplace",
                "MgmtCorruptThenRepair",
                "MgmtRepair",
                "MgmtBackupSpecificKeyspaces",
                "MgmtBackup",
            ]
        )
        self.disruptions_list = self.shuffle_list_of_disruptions(self.disruptions_list)


class ManagerRcloneBackup(NemesisRunner):
    manager_operation = True
    disruptive = False
    supports_high_disk_utilization = False

    def call_next_nemesis(self):
        self.disrupt_manager_backup(object_storage_upload_mode=ObjectStorageUploadMode.RCLONE, label="rclone_backup")


class ManagerNativeBackup(NemesisRunner):
    manager_operation = True
    disruptive = False
    supports_high_disk_utilization = False

    def call_next_nemesis(self):
        self.disrupt_manager_backup(object_storage_upload_mode=ObjectStorageUploadMode.NATIVE, label="native_backup")


class EnableDisableTableEncryptionAwsKmsProviderMonkey(NemesisRunner):
    disruptive = True
    kubernetes = False  # Enable it when EKS SCT code starts supporting the KMS service

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.disruptions_list = self.build_disruptions_by_name(
            [
                "EnableDisableTableEncryptionAwsKmsProviderWithoutRotationMonkey",
                "EnableDisableTableEncryptionAwsKmsProviderWithRotationMonkey",
            ]
        )
        self.disruptions_list = self.shuffle_list_of_disruptions(self.disruptions_list)


class CategoricalMonkey(NemesisRunner):
    """Randomly picks disruptions to execute using the given categorical distribution.

    Each disruption is assigned a weight. The probability that a disruption D with weight W
    will be executed is W / T, where T is the sum of weights of all disruptions.

    The distribution is passed into the monkey's constructor as a dictionary.
    Keys in the dictionary are names of the disruption methods (from the `Nemesis` class)
    e.g. `disrupt_hard_reboot_node`. The value for each key is the weight of this disruption.
    You can omit the ``disrupt_'' prefix from the key, e.g. `hard_reboot_node`.

    A default weight can be passed; it will be assigned to each disruption that is not listed.
    In particular if the default weight is 0 then the unlisted disruptions won't be executed.
    """

    def get_disruption_distribution(self, dist: dict, default_weight: float) -> Tuple[List[Callable], List[float]]:
        def is_nonnegative_number(val):
            try:
                val = float(val)
            except ValueError:
                return False
            else:
                return val >= 0

        all_methods = {
            method.__class__.__name__: method for method in self.build_disruptions_by_selector(self.nemesis_selector)
        }

        population: List[Callable] = []
        weights: List[float] = []
        listed_methods: Set[str] = set()

        for _name, _weight in dist.items():
            name = str(_name)
            if _name not in all_methods.keys():
                raise ValueError(f"'{name}' is not a valid disruption. All methods: {all_methods.keys()}")

            if not is_nonnegative_number(_weight):
                raise ValueError(
                    "Each disruption weight must be a non-negative number. '{weight}' is not a valid weight."
                )

            weight = float(_weight)
            if weight > 0:
                population.append(all_methods[_name])
                weights.append(weight)
            listed_methods.add(_name)

        if default_weight > 0:
            for method_name, method in all_methods.items():
                if method_name not in listed_methods:
                    population.append(method)
                    weights.append(default_weight)

        if not population:
            raise ValueError("There must be at least one disruption with a positive weight.")

        return population, weights

    def __init__(self, tester_obj, termination_event, dist: dict, *args, default_weight: float = 1, **kwargs):
        super().__init__(tester_obj, termination_event, *args, **kwargs)
        self.random = random.Random(self.nemesis_seed)
        self.disruption_distribution = self.get_disruption_distribution(dist, default_weight)

    def select_next_nemesis(self):
        population, weights = self.disruption_distribution
        assert len(population) == len(weights) and population

        return self.random.choices(population, weights=weights)[0]

    def call_next_nemesis(self):
        """Override parent method to change how nemesis are executed"""
        self.execute_nemesis(self.select_next_nemesis())


class MdcChaosMonkey(NemesisRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.disruptions_list = self.build_disruptions_by_name(
            ["CorruptThenRepairMonkey", "NoCorruptRepairMonkey", "DecommissionMonkey"]
        )
        self.disruptions_list = self.shuffle_list_of_disruptions(self.disruptions_list)
