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

from typing import Type, Protocol, Optional, runtime_checkable

from sdcm.sct_events import Severity, SctEventProtocol
from sdcm.sct_events.base import InformationalEvent


@runtime_checkable
class ClusterHealthValidatorEventClusterHealthCheck(SctEventProtocol, Protocol):
    info: Type[SctEventProtocol]
    done: Type[SctEventProtocol]


class ClusterHealthValidatorEvent(InformationalEvent, abstract=True):
    MonitoringStatus: Type[SctEventProtocol]
    NodeStatus: Type[SctEventProtocol]
    NodePeersNulls: Type[SctEventProtocol]
    NodeSchemaVersion: Type[SctEventProtocol]
    NodesNemesis: Type[SctEventProtocol]
    Done: Type[SctEventProtocol]
    Info: Type[SctEventProtocol]

    def __init__(self,
                 node=None,
                 message: Optional[str] = None,
                 error: Optional[str] = None,
                 severity=Severity.ERROR) -> None:
        super().__init__(severity=severity)

        self.node = str(node)
        self.error = error if error else ""
        self.message = message if message else ""

    @property
    def msgfmt(self) -> str:
        if self.severity in (Severity.NORMAL, Severity.WARNING, ):
            return super().msgfmt + ": type={0.type} node={0.node} message={0.message}"
        elif self.severity in (Severity.ERROR, Severity.CRITICAL, ):
            return super().msgfmt + ": type={0.type} node={0.node} error={0.error}"
        return super().msgfmt


ClusterHealthValidatorEvent.add_subevent_type("NodeStatus")
ClusterHealthValidatorEvent.add_subevent_type("NodePeersNulls")
ClusterHealthValidatorEvent.add_subevent_type("NodeSchemaVersion")
ClusterHealthValidatorEvent.add_subevent_type("NodesNemesis")
ClusterHealthValidatorEvent.add_subevent_type("MonitoringStatus")
ClusterHealthValidatorEvent.add_subevent_type("Done", severity=Severity.NORMAL)
ClusterHealthValidatorEvent.add_subevent_type("Info", severity=Severity.NORMAL)


class DataValidatorEvent(InformationalEvent, abstract=True):
    DataValidator: Type[SctEventProtocol]
    ImmutableRowsValidator: Type[SctEventProtocol]
    UpdatedRowsValidator: Type[SctEventProtocol]
    DeletedRowsValidator: Type[SctEventProtocol]

    def __init__(self,
                 message: Optional[str] = None,
                 error: Optional[str] = None,
                 severity: Severity = Severity.ERROR) -> None:
        super().__init__(severity=severity)

        self.error = error if error else ""
        self.message = message if message else ""

    @property
    def msgfmt(self) -> str:
        if self.severity in (Severity.NORMAL, Severity.WARNING, ):
            return super().msgfmt + ": type={0.type} message={0.message}"
        elif self.severity in (Severity.ERROR, Severity.CRITICAL, ):
            return super().msgfmt + ": type={0.type} error={0.error}"
        return super().msgfmt


DataValidatorEvent.add_subevent_type("DataValidator")
DataValidatorEvent.add_subevent_type("ImmutableRowsValidator")
DataValidatorEvent.add_subevent_type("UpdatedRowsValidator")
DataValidatorEvent.add_subevent_type("DeletedRowsValidator")


__all__ = ("ClusterHealthValidatorEvent", "DataValidatorEvent", )
