"""
NemesisRunner override, also known as Complex Nemesis
Runners can be used in nemesis_class_names config option
"""

import time
from typing import Callable, List, Set, Tuple

from sdcm.exceptions import NemesisPassCompleted
from sdcm.mgmt.common import ObjectStorageUploadMode
from sdcm.nemesis import NemesisBaseClass, NemesisRunner
from sdcm.sct_events.system import InfoEvent


class SisyphusMonkey(NemesisRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.disruptions_list = self.build_disruptions_by_selector(self.nemesis_selector)
        self.disruptions_list = self.shuffle_list_of_disruptions(self.disruptions_list)


class CategorySweepMonkey(NemesisRunner):
    """Runs every nemesis exactly once, category by category, then stops the nemesis thread.

    Where SisyphusMonkey shuffles the whole set and cycles it forever, this runner is a coverage
    sweep: the nemesis are grouped into the categories below, in that order, and each one runs
    once. An InfoEvent announces every category as its first nemesis starts, and a final event
    marks the completed pass, so a truncated run is distinguishable from a finished one.

    Categories are matched in order and a nemesis joins the first one it matches, so they stay
    disjoint. This matters for the nemesis that carry both schema_changes and topology_changes:
    they are swept as schema changes.

    Two config options behave differently here than under SisyphusMonkey:

      - nemesis_multiply_factor is ignored. Repeating the list contradicts "every nemesis once".
      - nemesis_selector still applies, intersected with every category, so a narrowed set is
        swept in the same category order.

    Order inside a category is alphabetical by class name rather than shuffled, so a sweep is
    reproducible without relying on nemesis_seed.
    """

    CATEGORIES: Tuple[Tuple[str, str], ...] = (
        ("schema-changes", "schema_changes"),
        ("topology-changes", "topology_changes and not schema_changes"),
        ("other-disruptive", "disruptive and not topology_changes and not schema_changes"),
        ("rest", "not disruptive and not topology_changes and not schema_changes"),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.category_by_nemesis: dict[str, str] = {}
        self.disruptions_list = self.build_categorized_disruptions()
        self._swept_count = 0
        self._current_category = None

    def build_categorized_disruptions(self) -> List[NemesisBaseClass]:
        """Collect the nemesis of every category into one list, in category order."""
        ordered = []
        for label, category_selector in self.CATEGORIES:
            selector = (
                f"({self.nemesis_selector}) and ({category_selector})" if self.nemesis_selector else category_selector
            )
            members = sorted(
                self.build_disruptions_by_selector(selector), key=lambda nemesis: nemesis.__class__.__name__
            )
            for nemesis in members:
                self.category_by_nemesis[nemesis.__class__.__name__] = label
            ordered.extend(members)
            self.log.info(
                "Nemesis category %r: %s", label, [nemesis.__class__.__name__ for nemesis in members] or "empty"
            )
        return ordered

    def call_next_nemesis(self):
        """Run the next nemesis of the sweep, or end the pass once the list is exhausted.

        precheck_nemesis() prunes disruptions_list in place before the first call and keeps its
        order, so the categories stay contiguous and a category is announced whenever the label
        changes from one nemesis to the next.
        """
        assert self.disruptions_list, "no nemesis were selected"
        if self._swept_count >= len(self.disruptions_list):
            raise NemesisPassCompleted(f"{self} completed its sweep of {self._swept_count} nemesis")

        nemesis = self.disruptions_list[self._swept_count]
        self._swept_count += 1
        category = self.category_by_nemesis.get(nemesis.__class__.__name__)
        if category != self._current_category:
            self._current_category = category
            InfoEvent(message=f"{self} starting nemesis category {category!r}").publish()
        self.execute_nemesis(nemesis)
        # Signalled here rather than only on the next call so a finished sweep does not idle away
        # one nemesis_interval after its last nemesis. A nemesis that ends by raising - a skip, a
        # teardown kill, a failure - keeps its own exception, and the guard above ends the sweep on
        # the following call instead.
        if self._swept_count >= len(self.disruptions_list):
            raise NemesisPassCompleted(f"{self} completed its sweep of {self._swept_count} nemesis")


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
        population, weights = self.get_disruption_distribution(dist, default_weight)
        self._weight_by_nemesis = dict(zip(population, weights))
        self.disruptions_list = population
        self.disruption_distribution = (population, weights)

    def precheck_nemesis(self) -> list[tuple[str, str]]:
        """Prune infeasible members from disruption_distribution too, via disruptions_list.

        CategoricalMonkey keeps its executable candidates in disruption_distribution rather
        than disruptions_list, so the base precheck alone would leave infeasible weighted
        candidates selectable by select_next_nemesis(). Mirroring the population into
        disruptions_list lets the base pruning run once, then this resyncs the weights to
        match the survivors.
        """
        excluded = super().precheck_nemesis()
        self.disruption_distribution = (
            list(self.disruptions_list),
            [self._weight_by_nemesis[nemesis] for nemesis in self.disruptions_list],
        )
        return excluded

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
