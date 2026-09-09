# Baremetal backend

[← All configuration options](../configuration_options.md)

Running against pre-existing hosts that SCT does not provision.

**7 options.**


<a id="db_nodes_private_ip"></a>

## **db_nodes_private_ip** / SCT_DB_NODES_PRIVATE_IP

Private IP addresses of DB nodes. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="db_nodes_public_ip"></a>

## **db_nodes_public_ip** / SCT_DB_NODES_PUBLIC_IP

Public IP addresses of DB nodes. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="loaders_private_ip"></a>

## **loaders_private_ip** / SCT_LOADERS_PRIVATE_IP

Private IP addresses of loader nodes. Loaders are used for running stress tests or other workloads against the DB. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="loaders_public_ip"></a>

## **loaders_public_ip** / SCT_LOADERS_PUBLIC_IP

Public IP addresses of loader nodes. These IPs are used for accessing the loaders from outside the private network. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="monitor_nodes_private_ip"></a>

## **monitor_nodes_private_ip** / SCT_MONITOR_NODES_PRIVATE_IP

Private IP addresses of monitor nodes. Monitoring nodes host monitoring tools like Prometheus and Grafana for DB performance monitoring. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="monitor_nodes_public_ip"></a>

## **monitor_nodes_public_ip** / SCT_MONITOR_NODES_PUBLIC_IP

Public IP addresses of monitor nodes. These IPs are used for accessing the monitoring tools from outside the private network. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="s3_baremetal_config"></a>

## **s3_baremetal_config** / SCT_S3_BAREMETAL_CONFIG

Configuration for S3 in baremetal setups. This includes details such as endpoint URL, access key, secret key, and bucket name.

**default:** N/A

**type:** str (appendable)
