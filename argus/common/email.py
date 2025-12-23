
from typing import Any, Literal, TypedDict


class ReportSection(TypedDict):
    type: str
    options: dict[str, Any]


ReportSectionShortHand = Literal["main",
                                 "header",
                                 "packages",
                                 "logs",
                                 "cloud",
                                 "nemesis",
                                 "events",
                                 "screenshots",
                                 "custom_table",
                                 "custom_html",]


class RawAttachment(TypedDict):
    filename: str
    data: str # Base64 string


class RawReportSendRequest(TypedDict):
    schema_version: str
    run_id: str
    title: str
    recipients: list[str]
    sections: list[ReportSection]
    attachments: list[RawAttachment]
