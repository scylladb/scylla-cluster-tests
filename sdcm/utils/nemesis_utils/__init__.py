import enum
from uuid import uuid4


class NEMESIS_TARGET_POOLS(enum.Enum):
    data_nodes = "data_nodes"
    zero_nodes = "zero_nodes"
    all_nodes = "nodes"


class DefaultValue:
    """
    This is class is intended to be used as default value for the cases when None is not applicable
    """


def unique_disruption_name(disruption: str) -> str:
    """Generates a unique disruption label to be able to differentiate between multiple parallel disruptions."""
    if "_" in disruption:
        disruption = "".join(p.capitalize() for p in disruption.replace("disrupt_", "").split("_"))
    return f"{disruption}-{uuid4().hex[:8]}"
