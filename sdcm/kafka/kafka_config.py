from typing import Optional

from pydantic import Field, BaseModel, Extra


class ConnectorConfiguration(BaseModel):
    # general options
    connector_class: str = Field(alias="connector.class")
    topics: Optional[str]

    # scylla-cdc-source-connector options
    # see https://github.com/scylladb/scylla-cdc-source-connector?tab=readme-ov-file#configuration
    # and https://github.com/scylladb/scylla-cdc-source-connector?tab=readme-ov-file#advanced-administration
    scylla_name: Optional[str] = Field(alias="scylla.name")
    scylla_table_names: Optional[str] = Field(alias="scylla.table.names")
    scylla_user: Optional[str] = Field(alias="scylla.user")
    scylla_password: Optional[str] = Field(alias="scylla.password")

    # kafka-connect-scylladb
    # see https://github.com/scylladb/kafka-connect-scylladb/blob/master/documentation/CONFIG.md
    scylladb_contact_points: Optional[str] = Field(alias="scylladb.contact.points")
    scylladb_keyspace: Optional[str] = Field(alias="scylladb.keyspace")
    scylladb_username: Optional[str] = Field(alias="scylladb.username")
    scylladb_password: Optional[str] = Field(alias="scylladb.password")

    class Config:
        extra = Extra.allow


class SctKafkaConfiguration(BaseModel):
    source: str = Field(
        exclude=True
    )  # url to specific release or hub version ex. 'hub:scylladb/scylla-cdc-source-connector:1.1.2'
    name: str  # connector name, each one should be named differently
    config: ConnectorConfiguration
