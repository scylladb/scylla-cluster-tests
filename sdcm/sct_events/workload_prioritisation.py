# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2021 ScyllaDB
import logging

from sdcm.sct_events import SctEventProtocol, Severity
from sdcm.sct_events.base import InformationalEvent

LOGGER = logging.getLogger(__name__)


class WorkloadPrioritisationEvent(InformationalEvent, abstract=True):
    CpuNotHighEnough: type[SctEventProtocol]
    RatioValidationEvent: type[SctEventProtocol]
    SlaTestResult: type[SctEventProtocol]
    EmptyPrometheusData: type[SctEventProtocol]

    def __init__(self,
                 message: str | None = None,
                 severity=Severity.ERROR) -> None:
        super().__init__(severity=severity)
        self.message = message if message else ""

    @property
    def msgfmt(self) -> str:
        return super().msgfmt + ": type={0.type} message={0.message}"


WorkloadPrioritisationEvent.add_subevent_type("CpuNotHighEnough")
WorkloadPrioritisationEvent.add_subevent_type("RatioValidationEvent")
WorkloadPrioritisationEvent.add_subevent_type("SlaTestResult")
WorkloadPrioritisationEvent.add_subevent_type("EmptyPrometheusData")

__all__ = ("WorkloadPrioritisationEvent",)
