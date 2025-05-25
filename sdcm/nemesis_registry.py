import ast
import inspect
import re
from enum import Enum
from functools import lru_cache
from typing import List, TypeVar, Callable, Dict, Any

from sdcm.utils.ast_utils import BooleanEvaluator

DISRUPT_PATTERN = re.compile(r"self\.(?P<method_name>disrupt_[0-9A-Za-z_]+?)\(.*\)", flags=re.MULTILINE)
SourceType = TypeVar("SourceType")
DisruptMethod = Callable[[Any], None]


@lru_cache
def get_disrupt_method_from_class(nemesis_cls):
    """Returns disrupt method that is called inside the disrupt() method"""
    method_name = DISRUPT_PATTERN.search(inspect.getsource(nemesis_cls.disrupt))
    if method_name:
        return method_name.group("method_name")


class NemesisRegistry:
    """
    Class, that serves as a Nemesis discovery mechanism.
    Currently, the disrupt methods are carrier of the code, but the Nemesis subclasses are carriers of flags.
    This class searches through all subclasses and matches disrupt methods with proper subclass, without initializing them.
    All searches are done through a logical phrase (e.g. "not disruptive")
    """

    def __init__(self, base_class: SourceType, excluded_list: List[SourceType] | None = None):
        super().__init__()
        self.base_class = base_class
        self.excluded_list = excluded_list or []

    def filter_subclasses(self, list_of_nemesis: List[SourceType], logical_phrase: str | None = None) -> List[SourceType]:
        """
        It applies 'and' logic to filter,
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
            if not logical_phrase or evaluator.visit(expression_ast):
                nemesis_subclasses.append(nemesis)
        return nemesis_subclasses

    def get_disrupt_methods(self, logical_phrase: str | None = None) -> List[DisruptMethod]:
        """Return all disrupt methods that satisfy logical phrase"""
        return self.filter_subclasses(self.get_subclasses(), logical_phrase)
        # return self.extract_methods(subclasses)

    def get_subclasses(self) -> List[SourceType]:
        """Collects all known subclasses of Nemesis class"""
        tmp = self.base_class.__subclasses__()
        subclasses = []
        while tmp:
            for nemesis in tmp.copy():
                subclasses.append(nemesis)
                tmp.remove(nemesis)
                tmp.extend(nemesis.__subclasses__())
        return subclasses

    def gather_properties(self) -> Dict[SourceType, Dict[str, bool]]:
        """Return all properties for all known subclasses and their respective disrupt methods"""
        class_properties = {}
        for subclass in self.get_subclasses():
            properties = class_properties.setdefault(subclass.__name__, {})
            for attribute in subclass.__dict__.keys():
                if attribute[:2] != "__":
                    value = getattr(subclass, attribute)
                    if isinstance(value, Enum):
                        properties[attribute] = value.value
                    elif not callable(value):
                        properties[attribute] = value
        return class_properties
