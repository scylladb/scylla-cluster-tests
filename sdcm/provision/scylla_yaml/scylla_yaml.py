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
from difflib import unified_diff
from typing import Literal, List, Dict, Any, Union

import yaml
from pydantic import field_validator, BaseModel, ConfigDict

from sdcm.provision.scylla_yaml.auxiliaries import EndPointSnitchType, ServerEncryptionOptions, ClientEncryptionOptions, \
    SeedProvider, RequestSchedulerOptions

logger = logging.getLogger(__name__)


class ScyllaYaml(BaseModel):
    """Model for scylla.yaml configuration.

    Only fields that require special validation or have a special type are defined explicitly.
    All other fields are handled dynamically through the "extra" model configuration.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="allow"  # Allow arbitrary fields to be set and retrieved
    )

    # Only keep fields that have specific validators or special handling
    server_encryption_options: ServerEncryptionOptions = None
    client_encryption_options: ClientEncryptionOptions = None
    seed_provider: List[SeedProvider] = None
    endpoint_snitch: EndPointSnitchType | None = None
    request_scheduler_options: RequestSchedulerOptions = None  # None
    request_scheduler: Literal[
        'org.apache.cassandra.scheduler.NoScheduler',
        'org.apache.cassandra.scheduler.RoundRobinScheduler'
    ] | None = None
    authenticator: Literal[
        "org.apache.cassandra.auth.PasswordAuthenticator",
        "org.apache.cassandra.auth.AllowAllAuthenticator",
        "com.scylladb.auth.TransitionalAuthenticator",
        "com.scylladb.auth.SaslauthdAuthenticator"
    ] | None = None
    authorizer: Literal[
        "org.apache.cassandra.auth.AllowAllAuthorizer",
        "org.apache.cassandra.auth.CassandraAuthorizer",
        "com.scylladb.auth.TransitionalAuthorizer",
        "com.scylladb.auth.SaslauthdAuthorizer"
    ] | None = None

    @field_validator("endpoint_snitch", mode="before")
    @classmethod
    def set_endpoint_snitch(cls, endpoint_snitch: str):
        if endpoint_snitch is None:
            return endpoint_snitch
        if endpoint_snitch.startswith('org.apache.cassandra.locator.'):
            return endpoint_snitch
        return 'org.apache.cassandra.locator.' + endpoint_snitch

    @field_validator("request_scheduler", mode="before")
    @classmethod
    def set_request_scheduler(cls, request_scheduler: str):
        if request_scheduler is None:
            return request_scheduler
        if request_scheduler.startswith('org.apache.cassandra.scheduler.'):
            return request_scheduler
        return 'org.apache.cassandra.scheduler.' + request_scheduler

    @field_validator("authenticator", mode="before")
    @classmethod
    def set_authenticator(cls, authenticator: str):
        if authenticator is None:
            return authenticator
        if authenticator.startswith(('org.apache.cassandra.auth.', 'com.scylladb.auth.')):
            return authenticator
        if authenticator in ['PasswordAuthenticator', 'AllowAllAuthenticator']:
            return 'org.apache.cassandra.auth.' + authenticator
        if authenticator in ['TransitionalAuthenticator', 'SaslauthdAuthenticator']:
            return 'com.scylladb.auth.' + authenticator
        return authenticator

    @field_validator("authorizer", mode="before")
    @classmethod
    def set_authorizer(cls, authorizer: str):
        if authorizer is None:
            return authorizer
        if authorizer.startswith(('org.apache.cassandra.auth.', 'com.scylladb.auth.')):
            return authorizer
        if authorizer in ['AllowAllAuthorizer', 'CassandraAuthorizer']:
            return 'org.apache.cassandra.auth.' + authorizer
        if authorizer in ['TransitionalAuthorizer', 'SaslauthdAuthorizer']:
            return 'com.scylladb.auth.' + authorizer
        return authorizer

    def model_dump(
        self,
        *,
        explicit: List[str] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """Converts the model to a dictionary, including dynamically added parameters.

        Args:
            explicit: List of attributes that should be explicitly included even if they have default values
            **kwargs: Additional parameters passed to the Pydantic model_dump

        Returns:
            Dictionary containing all attributes of the model
        """
        to_dict = super().model_dump(**kwargs)
        if explicit:
            for required_attrs in explicit:
                to_dict[required_attrs] = getattr(self, required_attrs, None)
        return to_dict

    def update(self, *objects: Union['ScyllaYaml', dict]):
        """
        Do the same as dict.update, with one exception.
        It ignores whatever key if it's value equal to default
        This comes from module `attr` and probably could be tackled there
        """
        fields_names = self.__class__.model_fields
        for obj in objects:
            if isinstance(obj, ScyllaYaml):
                attrs = {*fields_names, *obj.model_extra.keys()}
                for attr_name in attrs:
                    attr_value = getattr(obj, attr_name, None)
                    if attr_value is not None and attr_value != getattr(self, attr_name, None):
                        setattr(self, attr_name, attr_value)
            elif isinstance(obj, dict):
                for attr_name, attr_value in obj.items():
                    setattr(self, attr_name, attr_value)
            else:
                raise ValueError("Only dict or ScyllaYaml is accepted")
        return self

    def diff(self, other: 'ScyllaYaml') -> str:
        """Generates a unified diff between this instance and another ScyllaYaml instance.

        Args:
            other: Another ScyllaYaml instance to compare with

        Returns:
            String containing the unified diff
        """
        self_str = yaml.safe_dump(self.model_dump(exclude_defaults=True, exclude_unset=True,
                                  exclude_none=True)).splitlines(keepends=True)
        other_str = yaml.safe_dump(other.model_dump(exclude_defaults=True, exclude_unset=True,
                                   exclude_none=True)).splitlines(keepends=True)
        return "".join(unified_diff(self_str, other_str))

    def __copy__(self):
        """Creates a copy of this instance.

        Returns:
            A new ScyllaYaml instance with the same attributes
        """
        return self.__class__(**self.model_dump(exclude_defaults=True, exclude_unset=True))

    copy = __copy__
