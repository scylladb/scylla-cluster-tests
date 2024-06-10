from dataclasses import dataclass
from typing import Optional


@dataclass
class TabletsConfiguration:
    enabled: Optional[bool] = None
    initial: Optional[int] = None

    def __str__(self):
        items = []
        for k, v in self.__dict__.items():
            if v is not None:
                value = str(v).lower() if isinstance(v, bool) else v
                items.append(f"'{k}': {value}")
        return '{' + ', '.join(items) + '}'
