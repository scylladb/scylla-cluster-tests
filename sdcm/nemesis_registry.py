import ast
import inspect
import re
from functools import lru_cache
from typing import List, Optional, TypeVar, Callable

from sdcm.utils.ast_utils import BooleanEvaluator

DISRUPT_PATTERN = re.compile(r"self\.(?P<method_name>disrupt_[0-9A-Za-z_]+?)\(.*\)", flags=re.MULTILINE)
SourceType = TypeVar("SourceType")
DisruptMethod = Callable[[], None]


@lru_cache
def get_disrupt_method_from_class(nemesis_cls):
    method_name = DISRUPT_PATTERN.search(inspect.getsource(nemesis_cls.disrupt))
    if method_name:
        return method_name.group("method_name")


class NemesisRegistry:
    def __init__(self, base_class: SourceType, kubernetes=False, excluded_list=None):
        super().__init__()
        self.base_class = base_class
        self.kubernetes = kubernetes
        self.excluded_list = excluded_list or []

    def filter_subclasses(self, list_of_nemesis: List[SourceType], logical_phrase: str | None = None) -> List[SourceType]:
        """
        It apply 'and' logic to filter,
            if any value in the filter does not match what nemeses have,
            nemeses will be filtered out.
        """
        nemesis_subclasses = []

        evaluator = BooleanEvaluator()
        if logical_phrase:
            expression_ast = ast.parse(logical_phrase, mode="eval")

        for nemesis in list_of_nemesis:
            if nemesis in self.excluded_list:
                continue
            evaluator.context = dict(**nemesis.__dict__, **{nemesis.__name__: True})
            if logical_phrase and "disrupt_" in logical_phrase and (method_name := get_disrupt_method_from_class(nemesis)):
                # if the `logical_phrase` has a method name of any disrupt method
                # we look it up for the specific class and add it to the context
                # so we can match on those as well
                # example: 'disrupt_create_index or disrupt_drop_index'
                evaluator.context[method_name] = True
            if (logical_phrase and evaluator.visit(expression_ast)) or not logical_phrase:
                nemesis_subclasses.append(nemesis)
        return nemesis_subclasses

    def get_subclasses(self, logical_phrase: str | None = None) -> List[SourceType]:
        """Collects all known subclasses of Nemesis class"""
        tmp = self.base_class.__subclasses__()
        subclasses = []
        while tmp:
            for nemesis in tmp.copy():
                subclasses.append(nemesis)
                tmp.remove(nemesis)
                tmp.extend(nemesis.__subclasses__())
        return self.filter_subclasses(subclasses, logical_phrase)

    def get_list_of_methods_by_flags(  # noqa: PLR0913
        self,
        disruptive: Optional[bool] = None,
        supports_high_disk_utilization: Optional[bool] = None,
        run_with_gemini: Optional[bool] = None,
        networking: Optional[bool] = None,
        kubernetes: Optional[bool] = None,
        limited: Optional[bool] = None,
        topology_changes: Optional[bool] = None,
        schema_changes: Optional[bool] = None,
        config_changes: Optional[bool] = None,
        free_tier_set: Optional[bool] = None,
        sla: Optional[bool] = None,
        manager_operation: Optional[bool] = None,
        zero_node_changes: Optional[bool] = None,
    ) -> List[str]:
        args = dict(
            disruptive=disruptive,
            supports_high_disk_utilization=supports_high_disk_utilization,
            run_with_gemini=run_with_gemini,
            networking=networking,
            kubernetes=kubernetes,
            limited=limited,
            topology_changes=topology_changes,
            schema_changes=schema_changes,
            config_changes=config_changes,
            free_tier_set=free_tier_set,
            sla=sla,
            manager_operation=manager_operation,
            zero_node_changes=zero_node_changes,
        )
        logical_phrase = " and ".join([key for key, val in args.items() if val])
        subclasses_list = self.get_subclasses(logical_phrase=logical_phrase)

        disrupt_methods_list = []
        for subclass in subclasses_list:
            if method_name := get_disrupt_method_from_class(subclass):
                disrupt_methods_list.append(method_name)
        # self.log.debug("Gathered subclass methods: {}".format(disrupt_methods_list))
        return disrupt_methods_list

    def get_list_of_methods_compatible_with_backend(
        self,
        disruptive: Optional[bool] = None,
        supports_high_disk_utilization: Optional[bool] = None,
        run_with_gemini: Optional[bool] = None,
        networking: Optional[bool] = None,
        limited: Optional[bool] = None,
        topology_changes: Optional[bool] = None,
        schema_changes: Optional[bool] = None,
        config_changes: Optional[bool] = None,
        free_tier_set: Optional[bool] = None,
        manager_operation: Optional[bool] = None,
        zero_node_changes: Optional[bool] = None,
    ) -> List[str]:
        return self.get_list_of_methods_by_flags(
            disruptive=disruptive,
            supports_high_disk_utilization=supports_high_disk_utilization,
            run_with_gemini=run_with_gemini,
            networking=networking,
            kubernetes=self.kubernetes or None,
            limited=limited,
            topology_changes=topology_changes,
            schema_changes=schema_changes,
            config_changes=config_changes,
            free_tier_set=free_tier_set,
            manager_operation=manager_operation,
            zero_node_changes=zero_node_changes,
        )

    def get_list_of_subclasses_by_property_name(self, filter_logical_phrase: str | None):
        subclasses_list = self.get_subclasses(logical_phrase=filter_logical_phrase)
        return subclasses_list

    def get_list_of_disrupt_methods(self, subclasses_list, export_properties=False):
        disrupt_methods_names_list = []
        nemesis_classes = []
        all_methods_with_properties = []
        for subclass in subclasses_list:
            properties_list = []
            per_method_properties = {}
            for attribute in subclass.__dict__.keys():
                if attribute[:2] != "__":
                    value = getattr(subclass, attribute)
                    if not callable(value):
                        properties_list.append(f"{attribute} = {value}")
            if method_name_str := get_disrupt_method_from_class(subclass):
                disrupt_methods_names_list.append(method_name_str)
                nemesis_classes.append(subclass.__name__)
                if export_properties:
                    per_method_properties[method_name_str] = properties_list
                    all_methods_with_properties.append(per_method_properties)
                    all_methods_with_properties = sorted(all_methods_with_properties, key=lambda d: list(d.keys()))
        nemesis_classes.sort()
        disrupt_methods_objects_list = [attr[1] for attr in inspect.getmembers(
            self.base_class) if attr[0] in disrupt_methods_names_list and callable(attr[1])]
        return disrupt_methods_objects_list, all_methods_with_properties, nemesis_classes
