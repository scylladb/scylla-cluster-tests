"""
Module containing all network disruption nemesis classes.

Includes traffic-control-based disruptions (packet loss, corruption, delay,
bandwidth limiting, full block), iptables-based disruptions (reject/drop
packets on specific ports), and interface-level disruptions (stop/start
network interface).

Helper functions for iptables rule generation and command execution are
module-level utilities shared across the iptables-based nemesis classes.
"""

import contextlib
import logging
import time
from typing import List, Optional

from sdcm.db_stats import PrometheusDBStats
from sdcm.exceptions import UnsupportedNemesis
from sdcm.nemesis import NemesisBaseClass, target_all_nodes
from sdcm.sct_events import Severity
from sdcm.sct_events.filters import EventsSeverityChangerFilter
from sdcm.sct_events.group_common_events import (
    ignore_alternator_client_errors,
    suppress_expected_unavailability_errors,
)
from sdcm.sct_events.system import InfoEvent, CoreDumpEvent
from sdcm.utils.context_managers import DbNodeLogger
from sdcm.utils.issues import SkipPerIssues
from sdcm.utils.k8s.chaos_mesh import (
    NetworkBandwidthLimitExperiment,
    NetworkCorruptExperiment,
    NetworkDelayExperiment,
    NetworkPacketLossExperiment,
)

LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level helper functions
# ---------------------------------------------------------------------------


def install_iptables(target_node) -> None:
    """Install iptables on Ubuntu nodes where it is missing by default."""
    if target_node.distro.is_ubuntu:  # iptables is missing in a minimized Ubuntu installation
        target_node.install_package("iptables")


def iptables_randomly_get_random_matching_rule(rnd):
    """Randomly generate an iptables matching rule to match packets in a random manner.

    Args:
        rnd: A ``random.Random`` instance (e.g. ``self.runner.random``).

    Returns:
        A tuple of (textual_description, iptables_match_rule).
    """
    match_type = rnd.choice(
        ["statistic", "statistic", "statistic", "limit", "limit", "limit", ""]
        #  Make no matching rule less probable
    )

    if match_type == "statistic":
        mode = rnd.choice(["random", "nth"])
        if mode == "random":
            probability = rnd.choice(["0.0001", "0.001", "0.01", "0.1", "0.3", "0.6", "0.8", "0.9"])
            return (
                f"randomly chosen packet with {probability} probability",
                f"-m statistic --mode {mode} --probability {probability}",
            )
        elif mode == "nth":
            every = rnd.choice(["2", "4", "8", "16", "32", "64", "128"])
            return f"every {every} packet", f"-m statistic --mode {mode} --every {every} --packet 0"
    elif match_type == "limit":
        period = rnd.choice(["second", "minute"])
        pkts_per_period = rnd.choice({"second": [1, 5, 10], "minute": [2, 10, 40, 80]}.get(period))
        return (
            f"string of {pkts_per_period} very first packets every {period}",
            f"-m limit --limit {pkts_per_period}/{period}",
        )
    elif match_type == "connbytes":
        bytes_from = rnd.choice(["100", "200", "400", "800", "1600", "3200", "6400", "12800", "1280000"])
        return (
            f"every packet from connection that total byte counter exceeds {bytes_from}",
            f"-m connbytes --connbytes-mode bytes --connbytes-dir both --connbytes {bytes_from}",
        )
    return "every packet", ""


def iptables_randomly_get_disrupting_target(rnd):
    """Randomly generate an iptables target that can cause disruption.

    Args:
        rnd: A ``random.Random`` instance (e.g. ``self.runner.random``).

    Returns:
        A tuple of (textual_description, iptables_target).
    """
    target_type = rnd.choice(["REJECT", "DROP"])
    if target_type == "REJECT":
        reject_with = rnd.choice(
            [
                "icmp-net-unreachable",
                "icmp-host-unreachable",
                "icmp-port-unreachable",
                "icmp-proto-unreachable",
                "icmp-net-prohibited",
                "icmp-host-prohibited",
                "icmp-admin-prohibited",
            ]
        )
        return f"rejected with {reject_with}", f"{target_type} --reject-with {reject_with}"
    return "dropped", f"{target_type}"


def run_commands_wait_and_cleanup(
    node, log, name: str, start_commands: List[str], cleanup_commands: List[str] = None, wait_time: int = 0
):
    """Run commands on a target node, wait, and run cleanup commands.

    Args:
        node: Target node to execute commands on.
        log: Logger instance.
        name: Name of the nemesis for logging.
        start_commands: Commands to run on start.
        cleanup_commands: Commands to run to cleanup.
        wait_time: Waiting time in seconds.
    """
    cmd_executed = {}
    if cleanup_commands is None:
        cleanup_commands = []
    for cmd_num, cmd in enumerate(start_commands):
        try:
            node.remoter.run(cmd)
            log.debug(f"{name}: executed: {cmd}")
            cmd_executed[cmd_num] = True
            if wait_time:
                time.sleep(wait_time)
        except Exception as exc:  # noqa: BLE001
            cmd_executed[cmd_num] = False
            log.error(
                f"{name}: failed to execute start command {cmd} on node {node} due to the following error: {str(exc)}"
            )
    if not cmd_executed:
        return
    for cmd_num, cmd in enumerate(cleanup_commands):
        try:
            node.remoter.run(cmd)
        except Exception as exc:  # noqa: BLE001
            log.debug(
                f"{name}: failed to execute cleanup command {cmd} on node {node} due to the following error: {str(exc)}"
            )


def get_rate_limit_for_network_disruption(monitoring_set, target_node, rnd) -> Optional[str]:
    """Query Prometheus for recent network bandwidth and compute a random rate limit.

    Args:
        monitoring_set: The monitoring set containing Prometheus nodes.
        target_node: The target node to query metrics for.
        rnd: A ``random.Random`` instance (e.g. ``self.runner.random``).

    Returns:
        A rate limit string like "42mbps" or "350kbps", or None if no monitoring nodes.
    """
    if not monitoring_set.nodes:
        return None

    # get the last 10min avg network bandwidth used, and limit  30% to 70% of it
    prometheus_stats = PrometheusDBStats(host=monitoring_set.nodes[0].external_address)
    # If test runs with 2 network interfaces configuration, "node_network_receive_bytes_total" will be reported on device that
    # broadcast_address is configured on it
    query = 'avg(node_network_receive_bytes_total{instance=~".*?%s.*?", device="%s"})' % (
        target_node.ip_address,
        target_node.scylla_network_configuration.device,
    )
    now = time.time()
    results = prometheus_stats.query(query=query, start=now - 600, end=now)
    assert results, "no results for node_network_receive_bytes_total metric in Prometheus "
    received_bytes_over_time = [float(avg_rate) for _, avg_rate in results[0]["values"]]
    avg_bitrate_per_node = (received_bytes_over_time[-1] - received_bytes_over_time[0]) / 600
    avg_mpbs_per_node = avg_bitrate_per_node / 1024 / 1024

    if avg_mpbs_per_node > 10:
        min_limit = int(round(avg_mpbs_per_node * 0.30))
        max_limit = int(round(avg_mpbs_per_node * 0.70))
        rate_limit_suffix = "mbps"
    else:
        avg_kbps_per_node = avg_bitrate_per_node / 1024
        min_limit = int(round(avg_kbps_per_node * 0.30))
        max_limit = int(round(avg_kbps_per_node * 0.70))
        rate_limit_suffix = "kbps"

    return "{}{}".format(rnd.randrange(min_limit, max_limit), rate_limit_suffix)


# ---------------------------------------------------------------------------
# Monkey classes — Network disruption nemesis
# ---------------------------------------------------------------------------


@target_all_nodes
class RandomInterruptionNetworkMonkey(NemesisBaseClass):
    """Randomly applies network interruptions (loss, corruption, delay, or bandwidth limiting) to a node."""

    disruptive = True
    networking = True
    kubernetes = True

    additional_configs = ["configurations/network_config/two_interfaces.yaml"]
    # TODO: this definition should be removed when network configuration new mechanism will be supported by all backends.
    #  Now "ip_ssh_connections" is not supported for AWS and it is ignored.
    #  Test communication address (ip_ssh_connections) is defined as "public" for the relevant pipelines in "two_interfaces.yaml"
    additional_params = {"ip_ssh_connections": "public"}

    def disrupt(self):
        list_of_timeout_options = [10, 60, 120, 300, 500]
        if self.runner._is_it_on_kubernetes():
            self._disrupt_k8s(list_of_timeout_options)
            return

        if not self.runner.cluster.extra_network_interface:
            raise UnsupportedNemesis("for this nemesis to work, you need to set `extra_network_interface: True`")

        if not self.runner.target_node.install_traffic_control():
            raise UnsupportedNemesis("Traffic control package not installed on system")

        if not self.runner.target_node.scylla_network_configuration or (
            self.runner.target_node.scylla_network_configuration
            and not self.runner.target_node.scylla_network_configuration.device
        ):
            raise ValueError("The network device name is not recognized")

        rate_limit: Optional[str] = get_rate_limit_for_network_disruption(
            self.runner.monitoring_set, self.runner.target_node, rnd=self.runner.random
        )
        if not rate_limit:
            self.runner.log.warning(
                "NetworkRandomInterruption won't limit network bandwidth due to lack of monitoring nodes."
            )

        # random packet loss - between 1% - 15%
        loss_percentage = self.runner.random.randrange(1, 15)

        # random packet corruption - between 1% - 15%
        corrupt_percentage = self.runner.random.randrange(1, 15)

        # random packet delay - between 1s - 30s
        delay_in_secs = self.runner.random.randrange(1, 30)

        list_of_tc_options = [
            ("NetworkRandomInterruption_{}pct_loss".format(loss_percentage), "--loss {}%".format(loss_percentage)),
            (
                "NetworkRandomInterruption_{}pct_corrupt".format(corrupt_percentage),
                "--corrupt {}%".format(corrupt_percentage),
            ),
            (
                "NetworkRandomInterruption_{}sec_delay".format(delay_in_secs),
                "--delay {}s --delay-distro 500ms".format(delay_in_secs),
            ),
        ]
        if rate_limit:
            list_of_tc_options.append(
                ("NetworkRandomInterruption_{}_limit".format(rate_limit), "--rate {}".format(rate_limit))
            )

        option_name, selected_option = self.runner.random.choice(list_of_tc_options)
        wait_time = self.runner.random.choice(list_of_timeout_options)

        self.runner.actions_log.info(
            f"Network interruption start on {self.runner.target_node.name}, option: {option_name}, wait time: {wait_time}"
        )
        if self.runner.target_node.systemd_version < 256:
            context_manager = EventsSeverityChangerFilter(
                new_severity=Severity.WARNING,
                event_class=CoreDumpEvent,
                regex=r".*executable=.*networkd.*",
                extra_time_to_expiration=60,
            )
        else:
            context_manager = contextlib.nullcontext()

        InfoEvent(option_name).publish()
        self.runner.log.debug("NetworkRandomInterruption: [%s] for %dsec", selected_option, wait_time)
        with context_manager:
            self.runner.target_node.traffic_control(None)
            try:
                with DbNodeLogger(
                    self.runner.cluster.nodes,
                    f"network {option_name} interruption",
                    target_node=self.runner.target_node,
                ):
                    self.runner.target_node.traffic_control(selected_option)
                time.sleep(wait_time)
            finally:
                self.runner.target_node.traffic_control(None)
                self.runner.cluster.wait_all_nodes_un()
        self.runner.actions_log.info(f"Network random interruption finished on node {self.runner.target_node.name}")

    def _disrupt_k8s(self, list_of_timeout_options):
        """K8s variant: uses Chaos Mesh experiments for network disruption."""
        interruptions = ["delay", "loss", "corrupt"]
        rate_limit: Optional[str] = get_rate_limit_for_network_disruption(
            self.runner.monitoring_set, self.runner.target_node, rnd=self.runner.random
        )
        if not rate_limit:
            self.runner.log.warning(
                "NetworkRandomInterruption won't limit network bandwidth due to lack of monitoring nodes."
            )
        else:
            interruptions.append("rate")
        duration = f"{self.runner.random.choice(list_of_timeout_options)}s"
        interruption = self.runner.random.choice(interruptions)
        match interruption:
            case "delay":
                delay_in_msecs = self.runner.random.randrange(50, 300)
                jitter = delay_in_msecs * 0.2
                self.runner.actions_log.info(f"Interruption by network delay - delay: {delay_in_msecs} ms")
                experiment = NetworkDelayExperiment(
                    self.runner.target_node, duration, f"{delay_in_msecs}ms", correlation=20, jitter=f"{jitter}ms"
                )
            case "loss":
                loss_percentage = self.runner.random.randrange(1, 15)
                self.runner.actions_log.info(
                    f"Interruption by dropping network packets - loss_percentage: {loss_percentage}%"
                )
                experiment = NetworkPacketLossExperiment(
                    self.runner.target_node, duration, loss_percentage, correlation=20
                )
            case "corrupt":
                corrupt_percentage = self.runner.random.randrange(1, 15)
                self.runner.actions_log.info(
                    f"Interruption by corrupting network packets - corrupt_percentage: {corrupt_percentage}%"
                )
                experiment = NetworkCorruptExperiment(
                    self.runner.target_node, duration, corrupt_percentage, correlation=20
                )
            case "rate":
                rate, suffix = rate_limit[:-4], rate_limit[-4:]
                limit_base = int(rate) * 1024 * (1024 if suffix == "mbps" else 1)
                limit = limit_base * 20
                self.runner.actions_log.info(
                    f"Interruption by limiting network bandwidth - rate: {rate_limit}, limit: {limit}"
                )
                experiment = NetworkBandwidthLimitExperiment(
                    self.runner.target_node, duration, rate=rate_limit, limit=limit, buffer=10000
                )
        with (
            DbNodeLogger(
                self.runner.cluster.nodes,
                f"network {interruption} interruption",
                target_node=self.runner.target_node,
            ),
            self.runner.action_log_scope(f"Network interruption on {self.runner.target_node.name} for {duration}s"),
        ):
            experiment.start()
            experiment.wait_until_finished()
        self.runner.cluster.wait_all_nodes_un()


@target_all_nodes
class BlockNetworkMonkey(NemesisBaseClass):
    """Blocks all network traffic on a node using 100% packet loss."""

    disruptive = True
    networking = True
    kubernetes = True

    additional_configs = ["configurations/network_config/two_interfaces.yaml"]
    # TODO: this definition should be removed when network configuration new mechanism will be supported by all backends.
    #  Now "ip_ssh_connections" is not supported for AWS and it is ignored.
    #  Test communication address (ip_ssh_connections) is defined as "public" for the relevant pipelines in "two_interfaces.yaml"
    additional_params = {"ip_ssh_connections": "public"}

    def disrupt(self):
        list_of_timeout_options = [10, 60, 120, 300, 500]
        if self.runner._is_it_on_kubernetes():
            with self.runner.action_log_scope(f"Block network on {self.runner.target_node.name} node"):
                self._disrupt_k8s(list_of_timeout_options)
            return

        if not self.runner.cluster.extra_network_interface:
            raise UnsupportedNemesis("for this nemesis to work, you need to set `extra_network_interface: True`")

        if not self.runner.target_node.install_traffic_control():
            raise UnsupportedNemesis("Traffic control package not installed on system")

        if self.runner.target_node.systemd_version < 256:
            context_manager = EventsSeverityChangerFilter(
                new_severity=Severity.WARNING,
                event_class=CoreDumpEvent,
                regex=r".*executable=.*networkd.*",
                extra_time_to_expiration=60,
            )
        else:
            context_manager = contextlib.nullcontext()

        selected_option = "--loss 100%"
        wait_time = self.runner.random.choice(list_of_timeout_options)
        self.runner.log.debug("BlockNetwork: [%s] for %dsec", selected_option, wait_time)
        self.runner.actions_log.info(
            f"Network block start on {self.runner.target_node.name} node, wait_time: {wait_time}"
        )
        with context_manager, suppress_expected_unavailability_errors():
            self.runner.target_node.traffic_control(None)
            try:
                with DbNodeLogger(
                    self.runner.cluster.nodes,
                    "block network traffic",
                    target_node=self.runner.target_node,
                    additional_info=f"for {wait_time}sec",
                ):
                    self.runner.target_node.traffic_control(selected_option)
                time.sleep(wait_time)
            finally:
                self.runner.target_node.traffic_control(None)
                self.runner.cluster.wait_all_nodes_un()
        self.runner.actions_log.info(f"Network block finished on {self.runner.target_node.name} node")

    def _disrupt_k8s(self, list_of_timeout_options):
        """K8s variant: uses Chaos Mesh 100% packet loss to simulate full network block."""
        duration = f"{self.runner.random.choice(list_of_timeout_options)}s"
        experiment = NetworkPacketLossExperiment(self.runner.target_node, duration, probability=100)
        with suppress_expected_unavailability_errors():
            with DbNodeLogger(
                self.runner.cluster.nodes,
                "block network traffic",
                target_node=self.runner.target_node,
                additional_info=f"for {duration}sec",
            ):
                experiment.start()
            experiment.wait_until_finished()
            time.sleep(15)
            self.runner.cluster.wait_all_nodes_un()


@target_all_nodes
class RejectInterNodeNetworkMonkey(NemesisBaseClass):
    """Generates random firewall rule to drop/reject packets for inter-node communications, port 7000 and 7001."""

    disruptive = True
    networking = True
    free_tier_set = True

    def disrupt(self):
        # Temporary disable due to  https://github.com/scylladb/scylla/issues/6522
        if SkipPerIssues("https://github.com/scylladb/scylladb/issues/6522", self.runner.tester.params):
            raise UnsupportedNemesis("https://github.com/scylladb/scylladb/issues/6522")

        name = "RejectInterNodeNetwork"

        install_iptables(self.runner.target_node)

        textual_matching_rule, matching_rule = iptables_randomly_get_random_matching_rule(rnd=self.runner.random)
        textual_pkt_action, pkt_action = iptables_randomly_get_disrupting_target(rnd=self.runner.random)
        wait_time = self.runner.random.choice([10, 60, 120, 300, 500])

        InfoEvent(
            f"{name} {textual_matching_rule} that belongs to "
            "inter node communication connections (port=7000 and 7001) will be"
            f" {textual_pkt_action} for {wait_time}s"
        ).publish()

        # because of https://github.com/scylladb/scylla/issues/5802, we ignore YCSB client errors here
        with ignore_alternator_client_errors():
            return run_commands_wait_and_cleanup(
                self.runner.target_node,
                self.runner.log,
                name=name,
                start_commands=[
                    f"sudo iptables -t filter -A INPUT -p tcp --dport 7000 {matching_rule} -j {pkt_action}",
                    f"sudo iptables -t filter -A INPUT -p tcp --dport 7001 {matching_rule} -j {pkt_action}",
                ],
                cleanup_commands=[
                    f"sudo iptables -t filter -D INPUT -p tcp --dport 7000 {matching_rule} -j {pkt_action}",
                    f"sudo iptables -t filter -D INPUT -p tcp --dport 7001 {matching_rule} -j {pkt_action}",
                ],
                wait_time=wait_time,
            )


@target_all_nodes
class RejectNodeExporterNetworkMonkey(NemesisBaseClass):
    """Generates random firewall rule to drop/reject packets for node exporter connections, port 9100."""

    disruptive = True
    networking = True

    def disrupt(self):
        name = "RejectNodeExporterNetwork"

        install_iptables(self.runner.target_node)

        textual_matching_rule, matching_rule = iptables_randomly_get_random_matching_rule(rnd=self.runner.random)
        textual_pkt_action, pkt_action = iptables_randomly_get_disrupting_target(rnd=self.runner.random)
        wait_time = self.runner.random.choice([10, 60, 120, 300, 500])

        InfoEvent(
            f"{name} {textual_matching_rule} that belongs to "
            f"node-exporter(port=9100) connections will be {textual_pkt_action} for {wait_time}s"
        ).publish()

        return run_commands_wait_and_cleanup(
            self.runner.target_node,
            self.runner.log,
            name=name,
            start_commands=[f"sudo iptables -t filter -A INPUT -p tcp --dport 9100 {matching_rule} -j {pkt_action}"],
            cleanup_commands=[f"sudo iptables -t filter -D INPUT -p tcp --dport 9100 {matching_rule} -j {pkt_action}"],
            wait_time=wait_time,
        )


@target_all_nodes
class RejectThriftNetworkMonkey(NemesisBaseClass):
    """Generates random firewall rule to drop/reject packets for thrift connections, port 9100."""

    disruptive = True
    networking = True

    def disrupt(self):
        name = "RejectThriftNetwork"

        install_iptables(self.runner.target_node)

        textual_matching_rule, matching_rule = iptables_randomly_get_random_matching_rule(rnd=self.runner.random)
        textual_pkt_action, pkt_action = iptables_randomly_get_disrupting_target(rnd=self.runner.random)
        wait_time = self.runner.random.choice([10, 60, 120, 300, 500])

        InfoEvent(
            f"{name} {textual_matching_rule} that belongs to "
            f"Thrift(port=9160) connections will be {textual_pkt_action} for {wait_time}s"
        ).publish()

        return run_commands_wait_and_cleanup(
            self.runner.target_node,
            self.runner.log,
            name=name,
            start_commands=[f"sudo iptables -t filter -A INPUT -p tcp --dport 9160 {matching_rule} -j {pkt_action}"],
            cleanup_commands=[f"sudo iptables -t filter -D INPUT -p tcp --dport 9160 {matching_rule} -j {pkt_action}"],
            wait_time=wait_time,
        )


@target_all_nodes
class StopStartInterfacesNetworkMonkey(NemesisBaseClass):
    """Stops and restarts the secondary network interface on a node."""

    disruptive = True
    networking = True

    additional_configs = ["configurations/network_config/two_interfaces.yaml"]
    # TODO: this definition should be removed when network configuration new mechanism will be supported by all backends.
    #  Now "ip_ssh_connections" is not supported for AWS and it is ignored.
    #  Test communication address (ip_ssh_connections) is defined as "public" for the relevant pipelines in "two_interfaces.yaml"
    additional_params = {"ip_ssh_connections": "public"}

    def disrupt(self):
        if not self.runner.cluster.extra_network_interface:
            raise UnsupportedNemesis("for this nemesis to work, you need to set `extra_network_interface: True`")

        list_of_timeout_options = [10, 60, 120, 300, 500]
        wait_time = self.runner.random.choice(list_of_timeout_options)
        self.runner.log.debug("Taking down eth1 for %dsec", wait_time)

        try:
            self.runner.target_node.stop_network_interface()
            self.runner.actions_log.info(f"Taking {self.runner.target_node.name} node network interface down")
            time.sleep(wait_time)
        finally:
            self.runner.actions_log.info(f"Brigning {self.runner.target_node.name} node network interface up")
            self.runner.target_node.start_network_interface()
            with self.runner.action_log_scope("Wait all nodes up and normal"):
                self.runner.cluster.wait_all_nodes_un()
        self.runner.actions_log.info(f"Network interface down/up finished on {self.runner.target_node.name} node")
