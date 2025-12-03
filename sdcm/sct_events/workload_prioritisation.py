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
from typing import Type, Optional

from sdcm.sct_events import Severity, SctEventProtocol
from sdcm.sct_events.base import InformationalEvent

LOGGER = logging.getLogger(__name__)


class WorkloadPrioritisationEvent(InformationalEvent, abstract=True):
    CpuNotHighEnough: Type[SctEventProtocol]
    RatioValidationEvent: Type[SctEventProtocol]
    SlaTestResult: Type[SctEventProtocol]
    EmptyPrometheusData: Type[SctEventProtocol]

    def __init__(self, message: Optional[str] = None, severity=Severity.ERROR) -> None:
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
