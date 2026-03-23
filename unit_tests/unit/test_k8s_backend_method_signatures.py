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

"""Verify that K8s backend class method overrides remain signature-compatible with their cloud-provider parent classes.

Regression reference: https://github.com/scylladb/scylla-cluster-tests/pull/14113
MonitorSetEKS._create_instances did not accept the `ami_id` kwarg that was added to AWSCluster._create_instances,
causing a TypeError at runtime. These tests catch such signature drift automatically.
"""

import inspect

import pytest

from sdcm.cluster_aws import AWSCluster
from sdcm.cluster_gce import GCECluster
from sdcm.cluster_k8s import ScyllaPodCluster
from sdcm.cluster_k8s.eks import EksScyllaPodCluster, EksScyllaPodContainer, MonitorSetEKS
from sdcm.cluster_k8s.gke import GkeScyllaPodCluster, GkeScyllaPodContainer, MonitorSetGKE


def _get_parent_method(child_cls, method_name):
    """Walk the MRO to find the first parent class (after the child) that defines the method."""
    mro = inspect.getmro(child_cls)
    for parent_cls in mro[1:]:
        if method_name in parent_cls.__dict__:
            return parent_cls
    return None


def _check_signature_compatibility(parent_cls, child_cls, method_name):
    """Check that *child_cls.method_name* can be called with any argument combination valid for parent.

    Rules enforced:
    1. Every **keyword** parameter of the parent must also be accepted by the child
       (either as an explicit parameter or via ``**kwargs``).

    Returns a list of human-readable error strings (empty == compatible).
    """
    parent_method = getattr(parent_cls, method_name)
    child_method = getattr(child_cls, method_name)

    parent_sig = inspect.signature(parent_method)
    child_sig = inspect.signature(child_method)

    parent_params = dict(parent_sig.parameters)
    child_params = dict(child_sig.parameters)

    # Remove 'self' from both
    parent_params.pop("self", None)
    child_params.pop("self", None)

    errors = []

    # Does the child accept **kwargs? If so, any extra parent keyword is accepted.
    child_has_var_keyword = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in child_params.values())

    # Check that every parent parameter is accepted by the child
    for name, parent_param in parent_params.items():
        if parent_param.kind == inspect.Parameter.VAR_POSITIONAL:
            continue  # *args — skip
        if parent_param.kind == inspect.Parameter.VAR_KEYWORD:
            continue  # **kwargs — skip

        if name not in child_params:
            if child_has_var_keyword:
                continue  # Child captures it via **kwargs
            errors.append(
                f"Parent {parent_cls.__name__}.{method_name}() has parameter '{name}' "
                f"but {child_cls.__name__}.{method_name}() does not accept it"
            )

    return errors


# ---------------------------------------------------------------------------
# Test data: (child_class, parent_class, method_name)
# ---------------------------------------------------------------------------

_OVERRIDE_PAIRS = [
    # EKS overrides of AWS
    (MonitorSetEKS, AWSCluster, "_create_instances"),
    (MonitorSetEKS, AWSCluster, "_get_instances"),
    # EKS ScyllaPodCluster vs base ScyllaPodCluster
    (EksScyllaPodCluster, ScyllaPodCluster, "add_nodes"),
    # GKE overrides of GCE
    (MonitorSetGKE, GCECluster, "_get_instances"),
    # GKE ScyllaPodCluster vs base ScyllaPodCluster
    (GkeScyllaPodCluster, ScyllaPodCluster, "add_nodes"),
]


@pytest.mark.parametrize(
    "child_cls, parent_cls, method_name",
    _OVERRIDE_PAIRS,
    ids=[f"{c.__name__}.{m}-vs-{p.__name__}" for c, p, m in _OVERRIDE_PAIRS],
)
def test_k8s_override_signature_compatible_with_parent(child_cls, parent_cls, method_name):
    """Child K8s class override must accept every parameter its cloud-provider parent accepts."""

    assert hasattr(child_cls, method_name), f"{child_cls.__name__} does not have method '{method_name}'"
    assert hasattr(parent_cls, method_name), f"{parent_cls.__name__} does not have method '{method_name}'"

    errors = _check_signature_compatibility(parent_cls, child_cls, method_name)
    assert not errors, (
        f"Signature incompatibility between {child_cls.__name__}.{method_name}() "
        f"and {parent_cls.__name__}.{method_name}():\n" + "\n".join(f"  - {e}" for e in errors)
    )


def test_k8s_overrides_discovered_dynamically():
    """Dynamically discover all method overrides in K8s backend classes
    and verify they are signature-compatible with their cloud-provider parents.

    This catches cases where new overrides are added without matching the parent signature.
    """
    # Pairs of (K8s child class, cloud provider ancestor classes to check against)
    k8s_classes_and_cloud_ancestors = [
        (MonitorSetEKS, {"MonitorSetAWS", "AWSCluster", "BaseMonitorSet", "BaseCluster"}),
        (EksScyllaPodCluster, {"ScyllaPodCluster"}),
        (EksScyllaPodContainer, {"BaseScyllaPodContainer"}),
        (MonitorSetGKE, {"MonitorSetGCE", "GCECluster", "BaseMonitorSet", "BaseCluster"}),
        (GkeScyllaPodCluster, {"ScyllaPodCluster"}),
        (GkeScyllaPodContainer, {"BaseScyllaPodContainer"}),
    ]

    all_errors = []

    for child_cls, ancestor_names in k8s_classes_and_cloud_ancestors:
        # Get methods defined directly on the child class (not inherited)
        child_methods = {
            name
            for name, _ in inspect.getmembers(child_cls, predicate=inspect.isfunction)
            if name in child_cls.__dict__ and not name.startswith("__")
        }

        for method_name in child_methods:
            parent_cls = _get_parent_method(child_cls, method_name)
            if parent_cls is None:
                continue  # New method, not an override
            if parent_cls.__name__ not in ancestor_names:
                continue  # Not a cloud-provider ancestor

            errors = _check_signature_compatibility(parent_cls, child_cls, method_name)
            for err in errors:
                all_errors.append(f"{child_cls.__name__} vs {parent_cls.__name__}: {err}")

    assert not all_errors, (
        "K8s backend method overrides have signature incompatibilities with "
        "cloud-provider parents:\n" + "\n".join(f"  - {e}" for e in all_errors)
    )
