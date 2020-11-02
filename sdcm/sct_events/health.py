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
# Copyright (c) 2020 ScyllaDB

from typing import Type, Protocol, Optional, runtime_checkable

from sdcm.sct_events import Severity, SctEventProtocol
from sdcm.sct_events.base import SeverityLevelProtocol, ValidatorEvent


@runtime_checkable
class ClusterHealthValidatorEventClusterHealthCheck(SctEventProtocol, Protocol):
    info: Type[SctEventProtocol]
    done: Type[SctEventProtocol]


class ClusterHealthValidatorEvent(ValidatorEvent, abstract=True):
    NodeStatus: Type[SeverityLevelProtocol]
    NodePeersNulls: Type[SeverityLevelProtocol]
    NodeSchemaVersion: Type[SeverityLevelProtocol]
    NodesNemesis: Type[SeverityLevelProtocol]

    ClusterHealthCheck: Type[ClusterHealthValidatorEventClusterHealthCheck]

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
            return super().msgfmt + ": type={0.type} subtype={0.subtype} node={0.node} message={0.message}"
        elif self.severity in (Severity.ERROR, Severity.CRITICAL, ):
            return super().msgfmt + ": type={0.type} subtype={0.subtype} node={0.node} error={0.error}"
        return super().msgfmt


ClusterHealthValidatorEvent.add_subevent_type_with_severity_levels("NodeStatus")
ClusterHealthValidatorEvent.add_subevent_type_with_severity_levels("NodePeersNulls")
ClusterHealthValidatorEvent.add_subevent_type_with_severity_levels("NodeSchemaVersion")
ClusterHealthValidatorEvent.add_subevent_type_with_severity_levels("NodesNemesis")

ClusterHealthValidatorEvent.add_subevent_type("ClusterHealthCheck", abstract=True)
ClusterHealthValidatorEvent.ClusterHealthCheck.add_subevent_type("info", severity=Severity.NORMAL)
ClusterHealthValidatorEvent.ClusterHealthCheck.add_subevent_type("done", severity=Severity.NORMAL)


class DataValidatorEvent(ValidatorEvent, abstract=True):
    DataValidator: Type[SeverityLevelProtocol]
    ImmutableRowsValidator: Type[SeverityLevelProtocol]
    UpdatedRowsValidator: Type[SeverityLevelProtocol]
    DeletedRowsValidator: Type[SeverityLevelProtocol]

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
            return super().msgfmt + ": type={0.type} subtype={0.subtype} message={0.message}"
        elif self.severity in (Severity.ERROR, Severity.CRITICAL, ):
            return super().msgfmt + ": type={0.type} subtype={0.subtype} error={0.error}"
        return super().msgfmt


DataValidatorEvent.add_subevent_type_with_severity_levels("DataValidator")
DataValidatorEvent.add_subevent_type_with_severity_levels("ImmutableRowsValidator")
DataValidatorEvent.add_subevent_type_with_severity_levels("UpdatedRowsValidator")
DataValidatorEvent.add_subevent_type_with_severity_levels("DeletedRowsValidator")


__all__ = ("ClusterHealthValidatorEvent", "DataValidatorEvent", )
