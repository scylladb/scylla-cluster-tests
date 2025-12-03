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

from typing import Type, Optional

from sdcm.sct_events import Severity, SctEventProtocol
from sdcm.sct_events.base import InformationalEvent
from sdcm.sct_events.continuous_event import ContinuousEvent


class ClusterHealthValidatorSubEvents(InformationalEvent, abstract=True):
    def __init__(self, node: str = None, message: str = None, error: str = None, severity: Severity = Severity.UNKNOWN):
        super().__init__(severity=severity)
        self.node = str(node) if node else ""
        self.message = message if message else ""
        self.error = error if error else ""


class ClusterHealthValidatorEvent(ContinuousEvent):
    MonitoringStatus: Type[SctEventProtocol]
    NodeStatus: Type[SctEventProtocol]
    NodePeersNulls: Type[SctEventProtocol]
    NodeSchemaVersion: Type[SctEventProtocol]
    NodesNemesis: Type[SctEventProtocol]
    ScyllaCloudClusterServerDiagnostic: Type[SctEventProtocol]
    Group0TokenRingInconsistency: Type[SctEventProtocol]

    def __init__(
        self, node=None, message: Optional[str] = None, error: Optional[str] = None, severity=Severity.NORMAL
    ) -> None:
        self.node = str(node) if node else ""
        self.error = error if error else ""
        self.message = message if message else ""
        super().__init__(severity=severity)

    @property
    def msgfmt(self) -> str:
        message = super().msgfmt
        if self.type:
            message = message + ": type={0.type}"
        if self.node:
            message = message + " node={0.node}"
        if self.severity in (
            Severity.NORMAL,
            Severity.WARNING,
        ):
            if self.message:
                message = message + " message={0.message}"
            return message
        elif self.severity in (
            Severity.ERROR,
            Severity.CRITICAL,
        ):
            if self.error:
                message = message + " error={0.error}"
            return message
        return message


ClusterHealthValidatorEvent.add_subevent_type(
    "NodeStatus", severity=Severity.ERROR, mixin=ClusterHealthValidatorSubEvents
)
ClusterHealthValidatorEvent.add_subevent_type(
    "NodePeersNulls", severity=Severity.ERROR, mixin=ClusterHealthValidatorSubEvents
)
ClusterHealthValidatorEvent.add_subevent_type(
    "NodeSchemaVersion", severity=Severity.ERROR, mixin=ClusterHealthValidatorSubEvents
)
ClusterHealthValidatorEvent.add_subevent_type(
    "NodesNemesis", severity=Severity.ERROR, mixin=ClusterHealthValidatorSubEvents
)
ClusterHealthValidatorEvent.add_subevent_type(
    "MonitoringStatus", severity=Severity.ERROR, mixin=ClusterHealthValidatorSubEvents
)
ClusterHealthValidatorEvent.add_subevent_type(
    "ScyllaCloudClusterServerDiagnostic", severity=Severity.ERROR, mixin=ClusterHealthValidatorSubEvents
)
ClusterHealthValidatorEvent.add_subevent_type(
    "Group0TokenRingInconsistency", severity=Severity.ERROR, mixin=ClusterHealthValidatorSubEvents
)


class DataValidatorEvent(InformationalEvent, abstract=True):
    DataValidator: Type[SctEventProtocol]
    ImmutableRowsValidator: Type[SctEventProtocol]
    UpdatedRowsValidator: Type[SctEventProtocol]
    DeletedRowsValidator: Type[SctEventProtocol]

    def __init__(
        self, message: Optional[str] = None, error: Optional[str] = None, severity: Severity = Severity.ERROR
    ) -> None:
        super().__init__(severity=severity)

        self.error = error if error else ""
        self.message = message if message else ""

    @property
    def msgfmt(self) -> str:
        if self.severity in (
            Severity.NORMAL,
            Severity.WARNING,
        ):
            return super().msgfmt + ": type={0.type} message={0.message}"
        elif self.severity in (
            Severity.ERROR,
            Severity.CRITICAL,
        ):
            return super().msgfmt + ": type={0.type} error={0.error}"
        return super().msgfmt


class PartitionRowsValidationEvent(InformationalEvent):
    def __init__(self, message: str, severity: Severity = Severity.NORMAL):
        super().__init__(severity)
        self.message = message

    @property
    def msgfmt(self) -> str:
        return super().msgfmt + ": message={0.message}"


DataValidatorEvent.add_subevent_type("DataValidator")
DataValidatorEvent.add_subevent_type("ImmutableRowsValidator")
DataValidatorEvent.add_subevent_type("UpdatedRowsValidator")
DataValidatorEvent.add_subevent_type("DeletedRowsValidator")


__all__ = ("ClusterHealthValidatorEvent", "DataValidatorEvent", "PartitionRowsValidationEvent")
