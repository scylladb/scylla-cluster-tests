# Alternator (DynamoDB API)

[← All configuration options](../configuration_options.md)

Scylla's DynamoDB-compatible API: the endpoint, write isolation, load-balancing and the
credentials the tests use against it.

**10 options.**


<a id="alternator_access_key_id"></a>

## **alternator_access_key_id** / SCT_ALTERNATOR_ACCESS_KEY_ID

the aws_access_key_id that would be used for alternator

**default:** N/A

**type:** str (appendable)


<a id="alternator_enforce_authorization"></a>

## **alternator_enforce_authorization** / SCT_ALTERNATOR_ENFORCE_AUTHORIZATION

If true, enable the authorization check in dynamodb api (alternator)

**default:** False

**type:** bool


<a id="alternator_loadbalancing"></a>

## **alternator_loadbalancing** / SCT_ALTERNATOR_LOADBALANCING

If true, enable native load balancing for alternator

**default:** False

**type:** bool


<a id="alternator_port"></a>

## **alternator_port** / SCT_ALTERNATOR_PORT

Port to configure for alternator in scylla.yaml

**default:** N/A

**type:** int


<a id="alternator_secret_access_key"></a>

## **alternator_secret_access_key** / SCT_ALTERNATOR_SECRET_ACCESS_KEY

the aws_secret_access_key that would be used for alternator

**default:** N/A

**type:** str (appendable)


<a id="alternator_test_table"></a>

## **alternator_test_table** / SCT_ALTERNATOR_TEST_TABLE

Dictionary of a test alternator table features:<br>name: str - the name of the table<br>lsi_name: str - the name of the local secondary index to create with a table<br>gsi_name: str - the name of the global secondary index to create with a table<br>tags: dict - the tags to apply to the created table<br>items: int - expected number of items in the table after prepare

**default:** N/A

**type:** dict | YAML/JSON string → dict


<a id="alternator_trust_all_certificates"></a>

## **alternator_trust_all_certificates** / SCT_ALTERNATOR_TRUST_ALL_CERTIFICATES

If true, trust all TLS certificates for alternator connections (for testing with self-signed certs)

**default:** True

**type:** bool


<a id="alternator_use_dns_routing"></a>

## **alternator_use_dns_routing** / SCT_ALTERNATOR_USE_DNS_ROUTING

If true, spawn a docker with a dns server for the ycsb loader to point to

**default:** True

**type:** bool


<a id="alternator_write_isolation"></a>

## **alternator_write_isolation** / SCT_ALTERNATOR_WRITE_ISOLATION

Set the write isolation for the alternator table, see https://github.com/scylladb/scylla/blob/master/docs/alternator/alternator.md#write-isolation-policies for more details

**default:** N/A

**type:** str (appendable)


<a id="dynamodb_primarykey_type"></a>

## **dynamodb_primarykey_type** / SCT_DYNAMODB_PRIMARYKEY_TYPE

Type of dynamodb table to create with range key or not

**default:** HASH

**type:** Literal['HASH', 'HASH_AND_RANGE']
