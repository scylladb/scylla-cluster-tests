#!/usr/bin/env python

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
# Copyright (c) 2016 ScyllaDB
import json
import random
import time
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor
from threading import Event

from cassandra.query import SimpleStatement, ConsistencyLevel

from sdcm.sct_events.system import InfoEvent, Severity
from sdcm.cluster import BaseNode, NodeSetupFailed, NodeSetupTimeout
from sdcm.rest.remote_curl_client import RemoteCurlClient
from longevity_test import LongevityTest
from sdcm.utils.raft.common import get_topology_coordinator_node


class CmdRpcStatus(RemoteCurlClient):
    def __init__(self, node: BaseNode):
        super().__init__(host="localhost:10000", endpoint="storage_service", node=node)

    def cmd_rpc_status(self):
        path = "raft_topology/cmd_rpc_status"
        return json.loads(self.run_remoter_curl(method="GET", path=path, params=None).stdout.strip())


class ErrorInjection(RemoteCurlClient):
    """ """

    def __init__(self, node: "BaseNode"):
        super().__init__(host="localhost:10000", endpoint="v2/error_injection", node=node)

    def list_injected_errors(self) -> List[str]:
        path = "injection"
        return json.loads(self.run_remoter_curl(method="GET", path=path, params=None).stdout.strip())

    def get_injected_error_details(self, error_name: str) -> Dict:
        path = f"injection/{error_name}"
        return json.loads(self.run_remoter_curl(method="GET", path=path, params=None).stdout.strip())

    def disable_errors(self):
        path = "injection"
        return json.loads(self.run_remoter_curl(method="DELETE", path=path, params=None).stdout.strip())

    def enable_error(self, error_name: str, one_shot: bool = False, data: dict[str, str] | None = None):
        path = f"injection/{error_name}"
        params = {"one_shot": str(one_shot).lower()}

        self.run_remoter_curl(method="POST", path=path, params=params, data=data)

    def send_message_to_error(self, error_name: str):
        path = f"injection/{error_name}/message"
        self.run_remoter_curl(method="POST", path=path, params=None)


class LongevityErrorInjectionTest(LongevityTest):
    """
    This class is used for reproducing longevity test failures. It inherits from LongevityTest and can override any of its methods if needed.
    """

    def test_custom_time(self):  # noqa: PLR0914,PLR0915,PLR0912
        """
        Run cassandra-stress with params defined in data_dir/scylla.yaml
        """
        stop_w_event = Event()

        def stuck_write(session, event):
            self.log.info("Send single request until trigger error injection")
            for i in range(1000):
                self.log.info("Sending write request number %s", i)
                try:
                    session.execute(
                        SimpleStatement(
                            f"INSERT INTO ks1.table1 (id, val) VALUES ({i}, {i});",
                            consistency_level=ConsistencyLevel.ALL,
                        )
                    )
                except Exception as exc:  # noqa: BLE001
                    self.log.info("Got exception while sending write request: %s", exc)
                if event.is_set():
                    self.log.info("Stopping sending write requests since event is set")
                    break
                time.sleep(0.5)

        nodes_by_host_id_before = {node.host_id: node for node in self.db_cluster.nodes}
        verification_node = get_topology_coordinator_node(self.db_cluster.nodes[0])
        self.log.info("Topology coordinator node is %s", verification_node.name)

        self.run_pre_create_keyspace()
        self.run_pre_create_schema()
        self.run_prepare_write_cmd()

        new_nodes = self.prepare_new_nodes(count=1, dc_idx=0, rack=0)

        self.log.info("Create new keyspaces and table for trigger error injection")
        with self.db_cluster.cql_connection_patient(node=verification_node) as session:
            query = "CREATE KEYSPACE ks1 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} and tablets = {'enabled': false};"
            session.execute(query)
            query = "CREATE TABLE ks1.table1 (id int primary key, val int);"
            session.execute(query)
            self.log.info("Pre-created table ks1.table1")

        logs_followers = []
        for node in self.db_cluster.nodes:
            logs_followers.append(
                {
                    "node": node,
                    "searcher": node.follow_system_log(
                        patterns=["storage_proxy_write_response_pause: waiting for message"]
                    ),
                }
            )

        injecting_node = random.choice([node for node in nodes_by_host_id_before.values() if node != verification_node])
        self.log.info("Inject error 'storage_proxy_write_response_pause' to node: %s", injecting_node.name)
        error_inj_client = ErrorInjection(node=injecting_node)
        current_errors = error_inj_client.list_injected_errors()
        self.log.info("Currently injected errors before injection: %s", current_errors)
        error_inj_client.enable_error(error_name="storage_proxy_write_response_pause", one_shot=True)
        injected_errors = error_inj_client.list_injected_errors()
        self.log.info("Currently injected errors: %s", injected_errors)
        assert "storage_proxy_write_response_pause" in injected_errors, "Error injection failed"

        with ThreadPoolExecutor(max_workers=2) as bg_pool:
            self.log.info("Start stuck write thread. Sending request while error injection will not be detected")
            session = self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]).__enter__()
            thread_w = bg_pool.submit(stuck_write, session, stop_w_event)
            self.log.info("Waiting the error injection trigger")
            st = time.time()
            stop = False
            while time.time() - st < 180:
                for follower in logs_followers:
                    self.log.info("Checking logs for error injection message %s", follower["node"].name)
                    if found := list(follower["searcher"]):
                        self.log.info("Found %s on node %s logs", found, follower["node"].name)
                        stop = True
                        break
                time.sleep(1)
                if stop:
                    break
            else:
                raise Exception("Did not find expected log message for error injection in any of the nodes logs")

            add_new_node_th = bg_pool.submit(self.start_nodes, nodes=new_nodes)
            stop_w_event.set()
            self.log.info("Waiting bootstrap to hang up")
            st = time.time()
            rps_status_client = CmdRpcStatus(node=verification_node)
            host_id = None
            while time.time() - st < 180:
                response = rps_status_client.cmd_rpc_status()
                self.log.info("RPC status response: %s", response)
                if "barrier_and_drain" in response:
                    host_id = response
                    break
                time.sleep(1)
            else:
                raise Exception("Did not find expected 'barrier_and_drain' status in RPC status response")

            assert host_id is not None, "Failed to get host id from RPC status"
            _, hostids = host_id.split()
            hostids = hostids.split(",")
            self.log.info("Host ids from RPC status: %s", hostids)
            if len(hostids) != 1:
                self.log.error("Expected exactly one host id in RPC status response, got: %s", hostids)

            stucked_nodes = [node for host_id, node in nodes_by_host_id_before.items() if host_id in hostids]
            if verification_node in stucked_nodes:
                self.log.info("Switch the verification node since it's one of the stucked nodes")
                verification_node = next(node for node in nodes_by_host_id_before.values() if node not in stucked_nodes)
                self.log.info("New verification node is %s", verification_node.name)

            st = time.time()
            stop = False
            while time.time() - st < 180:
                status = self.db_cluster.get_nodetool_status(verification_node, dc_aware=False)
                for node in new_nodes:
                    if node.ip_address in status and "UJ" in status[node.ip_address]["state"]:
                        self.log.info("Node %s stucked to UJ", node.name)
                        stop = True
                        break
                if stop:
                    break
            else:
                raise Exception("New node did not get stuck in UJ state as expected")

            self.log.info("Nodes with host ids from RPC status: %s", [n.name for n in stucked_nodes])

            statuses = verification_node.get_nodes_status()
            self.log.info(
                "Nodes status before rebooting stucked nodes: %s",
                {node.name: status for node, status in statuses.items()},
            )
            self.log.info("Send message to error injection")
            error_inj_client.send_message_to_error("storage_proxy_write_response_pause")
            self.log.info("Start sending customer payload")
            stress_queue = []
            keyspace_num = self.params.get("keyspace_num")
            stress_cmd = self.params.get("stress_cmd")
            self.assemble_and_run_all_stress_cmd(stress_queue, stress_cmd, keyspace_num)
            self.log.info("Customer payload is running, waiting 60 seconds before rebooting stucked nodes")
            time.sleep(60)

            for node in stucked_nodes:
                node.restart_scylla_server()

            statuses = verification_node.get_nodes_status()
            self.log.info(
                "Nodes status after rebooting stucked nodes: %s",
                {node.name: status for node, status in statuses.items()},
            )
            stop_w_event.clear()
            result = thread_w.result(timeout=300)
            self.log.info("Stuck write thread finished with result: %s", result)
            if exc := add_new_node_th.exception():
                self.log.error("Add new node thread raised an exception: %s", exc)
                for node in new_nodes:
                    find_bootstrap_failed = node.follow_system_log(
                        patterns=["init - Startup failed: std::runtime_error"], start_from_beginning=True
                    )
                    if list(find_bootstrap_failed):
                        self.log.info("Node %s failed to bootstrap as expected", node.name)
                    else:
                        InfoEvent(
                            message=f"Node {node.name} did not fail to bootstrap as expected check the logs",
                            severity=Severity.ERROR,
                        ).publish()

            self.db_cluster.check_nodes_up_and_normal()

            for stress in stress_queue:
                self.verify_stress_thread(stress)

    def prepare_new_nodes(self, count: int, dc_idx: int, rack: int | str, instance_type: str | None = None):
        self.log.info("Adding %s new nodes to cluster...", count)
        InfoEvent(message=f"StartEvent - Adding {count} new nodes to cluster").publish()
        add_node_func_args = {
            "count": count,
            "dc_idx": dc_idx,
            "enable_auto_bootstrap": True,
            "rack": rack,
            "instance_type": instance_type,
        }
        self.log.info(f"Add {count} new nodes on {rack} rack, {instance_type} type")
        new_nodes = self.db_cluster.add_nodes(**add_node_func_args)
        self.monitors.reconfigure_scylla_monitoring()
        try:
            for node in new_nodes:
                self.db_cluster.node_setup(node, verbose=True)

        except (NodeSetupFailed, NodeSetupTimeout):
            self.log.warning("TestConfig of the '%s' failed, removing them from list of nodes" % new_nodes)
            for node in new_nodes:
                self.db_cluster.nodes.remove(node)
            self.log.warning("Nodes will not be terminated. Please terminate manually!!!")
            raise
        return new_nodes

    def start_nodes(self, nodes: list[BaseNode]):
        # TODO - add stop waiting for node to be up in bootstrap failed.
        try:
            for node in nodes:
                self.log.info("Starting node %s", node.name)
                self.db_cluster.node_startup(node, verbose=True, timeout=1800)
                self.log.info("Node %s started", node.name)
        except Exception as exc:  # noqa: BLE001
            self.log.error("Failed to start node %s: %s", node.name, exc)
            raise
