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
# Copyright (c) 2026 ScyllaDB

"""Exception hierarchy for the trigger matrix.

Kept in its own leaf module so that every other submodule -- including `jenkins_client`,
which must not pull in the pydantic models -- can raise without creating an import cycle."""


class TriggerMatrixError(Exception):
    """Base exception for trigger matrix errors."""


class MatrixValidationError(TriggerMatrixError):
    """Raised when YAML matrix file fails validation."""


class JenkinsTriggerError(TriggerMatrixError):
    """Raised when a Jenkins job trigger fails."""
