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

# pylint: disable=too-many-arguments

import logging

import kubernetes as k8s

from sdcm.remote import LOCALRUNNER


KUBECTL_BIN = "kubectl"
HELM_BIN = "helm"


logging.getLogger("kubernetes.client.rest").setLevel(logging.INFO)


class KubernetesOps:
    @staticmethod
    def create_k8s_configuration(kluster):
        k8s_configuration = k8s.client.Configuration()
        if kluster.k8s_server_url:
            k8s_configuration.host = kluster.k8s_server_url
        else:
            k8s.config.load_kube_config(client_configuration=k8s_configuration)
        return k8s_configuration

    @classmethod
    def api_client(cls, kluster):
        conf = getattr(kluster, "k8s_configuration", None) or cls.create_k8s_configuration(kluster)
        return k8s.client.ApiClient(conf)

    @classmethod
    def core_v1_api(cls, kluster):
        return getattr(kluster, "_k8s_core_v1_api", None) or k8s.client.CoreV1Api(cls.api_client(kluster))

    @classmethod
    def list_pods(cls, kluster, namespace=None, **kwargs):
        if namespace is None:
            return cls.core_v1_api(kluster).list_pod_for_all_namespaces(watch=False, **kwargs).items
        return cls.core_v1_api(kluster).list_namespaced_pod(namespace=namespace, watch=False, **kwargs).items

    @classmethod
    def list_services(cls, kluster, namespace=None, **kwargs):
        if namespace is None:
            return cls.core_v1_api(kluster).list_service_all_namespaces(watch=False, **kwargs).items
        return cls.core_v1_api(kluster).list_namespaced_service(namespace=namespace, watch=False, **kwargs).items

    @staticmethod
    def kubectl(kluster, *command, namespace=None, timeout=300, remoter=None):
        cmd = [KUBECTL_BIN, ]
        if remoter is None:
            if kluster.k8s_server_url is not None:
                cmd.append(f"--server={kluster.k8s_server_url}")
            remoter = LOCALRUNNER
        if namespace:
            cmd.append(f"--namespace={namespace}")
        cmd.extend(command)
        return remoter.run(" ".join(cmd), timeout=timeout)

    @staticmethod
    def helm(kluster, *command, namespace=None, timeout=300, remoter=None):
        cmd = [HELM_BIN, ]
        if remoter is None:
            if kluster.k8s_server_url is not None:
                cmd.append(f"--kube-apiserver {kluster.k8s_server_url}")
            remoter = LOCALRUNNER
        if namespace:
            cmd.append(f"--namespace {namespace}")
        cmd.extend(command)
        return remoter.run(" ".join(cmd), timeout=timeout)

    @classmethod
    def apply_file(cls, kluster, config_path, namespace=None, timeout=300):
        cls.kubectl(kluster, "apply", "-f", f"<(envsubst<{config_path})", namespace=namespace, timeout=timeout)

    @classmethod
    def copy_file(cls, kluster, src, dst, container=None, timeout=300):  # pylint: disable=too-many-arguments
        command = ["cp", src, dst]
        if container:
            command.extend(("-c", container))
        cls.kubectl(kluster, *command, timeout=timeout)

    @classmethod
    def expose_pod_ports(cls, kluster, pod_name, ports, namespace=None, timeout=300):
        ports = ",".join(map(str, ports))
        cls.kubectl(kluster,
                    f"expose pod {pod_name} --type=LoadBalancer --port={ports} --name={pod_name}-loadbalancer",
                    namespace=namespace,
                    timeout=timeout)
