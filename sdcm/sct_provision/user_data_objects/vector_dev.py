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
# Copyright (c) 2025 ScyllaDB
from dataclasses import dataclass

from sdcm.provision.common.utils import configure_vector_target_script, install_vector_service, configure_backoff_timeout
from sdcm.sct_provision.user_data_objects import SctUserDataObject


@dataclass
class VectorDevUserDataObject(SctUserDataObject):

    @property
    def is_applicable(self) -> bool:
        return self.params.get('logs_transport') == 'vector'

    @property
    def script_to_run(self) -> str:
        script = configure_backoff_timeout()
        script += install_vector_service()
        host, port = self.test_config.get_logging_service_host_port()
        script += configure_vector_target_script(host=host, port=port)
        return script
