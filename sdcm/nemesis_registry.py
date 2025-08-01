import ast
import inspect
from typing import List, TypeVar, Callable, Dict, Any

from sdcm.utils.ast_utils import BooleanEvaluator

SourceType = TypeVar("SourceType")
DisruptMethod = Callable[[Any], None]


def get_members_from_class(cls: SourceType) -> List[str]:
    """
    Returns all members of the class, excluding methods and private attributes.
    """
    return [name for name, value in inspect.getmembers(cls, lambda a: not (inspect.isroutine(a))) if not name.startswith("_")]


def get_flags_from_class(cls: SourceType, members: List[str]) -> Dict[str, Any]:
    """
    Returns all flags from the class, excluding methods and private attributes.
    Flags are expected to be boolean or Enum values.
    """
    return {name: getattr(cls, name) for name in members}


class NemesisRegistry:
    """
    Class, that serves as a Nemesis discovery mechanism.
    Currently, the disrupt methods are carrier of the code, but the Nemesis subclasses are carriers of flags.
    This class searches through all subclasses and matches disrupt methods with proper subclass, without initializing them.
    All searches are done through a logical phrase (e.g. "not disruptive")
    """

    def __init__(self, base_class: SourceType, flag_class: Any, excluded_list: List[SourceType] | None = None):
        super().__init__()
        self.base_class = base_class
        self.flag_class = flag_class
        self.members = get_members_from_class(flag_class)
        self.excluded_list = excluded_list or []

    def filter_subclasses(self, logical_phrase: str | None = None) -> List[SourceType]:
        """
        It applies 'and' logic to filter,
            if any value in the filter does not match what nemeses have,
            nemeses will be filtered out.
        """
        nemesis_subclasses = []

        evaluator = BooleanEvaluator()
        if logical_phrase:
            expression_ast = ast.parse(logical_phrase, mode="eval")

        for nemesis in self.get_subclasses():
            context = get_flags_from_class(nemesis, self.members)
            context[nemesis.__name__] = True
            evaluator.context = context
            if not logical_phrase or evaluator.visit(expression_ast):
                nemesis_subclasses.append(nemesis)
        return nemesis_subclasses

    def get_subclasses(self) -> List[SourceType]:
        """Collects all known subclasses of Nemesis class"""
        tmp = self.base_class.__subclasses__()
        subclasses = []
        while tmp:
            for nemesis in tmp.copy():
                tmp.remove(nemesis)
                tmp.extend(nemesis.__subclasses__())
                if nemesis not in self.excluded_list and not inspect.isabstract(nemesis):
                    subclasses.append(nemesis)

        return subclasses

    def gather_properties(self) -> Dict[SourceType, Dict[str, bool]]:
        """Return all properties for all known subclasses and their respective disrupt methods"""
        class_properties = {}
        for subclass in self.get_subclasses():
            class_properties[subclass.__name__] = get_flags_from_class(subclass, self.members)
        return class_properties
