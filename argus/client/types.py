from dataclasses import dataclass


@dataclass(init=True, repr=True)
class CostItem:
    name: str
    category: str
    cost: float
    pricing_tier: str | None = None
    leaked: bool = False
