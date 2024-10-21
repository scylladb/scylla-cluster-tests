from dataclasses import dataclass


@dataclass(init=True, repr=True)
class Package:
    name: str
    version: str
    date: str
    revision_id: str
    build_id: str


@dataclass(init=True, repr=True)
class LogLink:
    log_name: str
    log_link: str


@dataclass(init=True, repr=True)
class EventsInfo:
    severity: str
    total_events: int
    messages: list[str]
