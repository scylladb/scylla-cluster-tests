import enum


class NEMESIS_TARGET_POOLS(enum.Enum):
    data_nodes = "data_nodes"
    zero_nodes = "zero_nodes"
    all_nodes = "nodes"


class DefaultValue:
    """
    This is class is intended to be used as default value for the cases when None is not applicable
    """
