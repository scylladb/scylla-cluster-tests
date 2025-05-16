from copy import deepcopy
from unittest import mock

from sdcm.utils.k8s import (
    HelmValues,
    KubernetesOps,
    ScyllaPodsIPChangeTrackerThread,
)


BASE_HELM_VALUES = {
    "no_nesting_key": "no_nesting_value",
    "nested_dict": {
        "first_nested_dict_key": "first_nested_dict_value",
        "second_nested_dict_key": "second_nested_dict_value",
    },
    "nested_list": [1, 2, 3],
}


def test_helm_values_init_with_dict_arg():
    helm_values = HelmValues(BASE_HELM_VALUES)
    assert helm_values.as_dict() == BASE_HELM_VALUES


def test_helm_values_init_with_kwargs():
    helm_values = HelmValues(**BASE_HELM_VALUES)
    assert helm_values.as_dict() == BASE_HELM_VALUES


def test_helm_values_get():
    helm_values = HelmValues(BASE_HELM_VALUES)
    assert helm_values.get("no_nesting_key") == "no_nesting_value"


def test_helm_values_get_nonexistent():
    helm_values = HelmValues(BASE_HELM_VALUES)
    assert helm_values.get("fake_key") is None


def test_helm_values_set_new():
    helm_values = HelmValues(BASE_HELM_VALUES)
    helm_values.set("new_key", "new_value")
    assert helm_values.get("new_key") == "new_value"


def test_helm_values_set_nested_new():
    helm_values = HelmValues(BASE_HELM_VALUES)
    helm_values.set("nested_dict.third_nested_dict_key", "third_nested_dict_value")
    data = helm_values.as_dict()
    assert "nested_dict" in data
    assert "third_nested_dict_key" in data["nested_dict"]
    assert data["nested_dict"]["third_nested_dict_key"] == "third_nested_dict_value"

    assert "first_nested_dict_key" in data["nested_dict"]
    assert data["nested_dict"]["first_nested_dict_key"] == "first_nested_dict_value"

    assert "second_nested_dict_key" in data["nested_dict"]
    assert data["nested_dict"]["second_nested_dict_key"] == "second_nested_dict_value"


def test_helm_values_set_override():
    helm_values = HelmValues(BASE_HELM_VALUES)
    helm_values.set("no_nesting_key", "custom_value")
    assert helm_values.get("no_nesting_key") == "custom_value"


def test_helm_values_set_nested_override():
    helm_values = HelmValues(BASE_HELM_VALUES)
    helm_values.set("nested_dict.first_nested_dict_key", "new_value")
    data = helm_values.as_dict()
    assert "nested_dict" in data
    assert "first_nested_dict_key" in data["nested_dict"]
    assert data["nested_dict"]["first_nested_dict_key"] == "new_value"


def test_helm_values_get_list():
    helm_values = HelmValues(BASE_HELM_VALUES)
    assert helm_values.get("nested_list") == [1, 2, 3]


def test_helm_values_get_by_list_index():
    helm_values = HelmValues(BASE_HELM_VALUES)
    assert helm_values.get("nested_list.[0]") == 1


def test_helm_delete_from_list():
    helm_values = HelmValues(deepcopy(BASE_HELM_VALUES))
    match = deepcopy(BASE_HELM_VALUES)
    del match['nested_list'][0]
    helm_values.delete("nested_list.[0]")
    assert helm_values == match


def test_helm_delete_from_dict():
    helm_values = HelmValues(deepcopy(BASE_HELM_VALUES))
    match = deepcopy(BASE_HELM_VALUES)
    del match['nested_dict']['first_nested_dict_key']
    helm_values.delete("nested_dict.first_nested_dict_key")
    assert helm_values == match


def test_helm_values_try_set_by_list_index():
    helm_values = HelmValues(BASE_HELM_VALUES)
    try:
        helm_values.set("nested_list[0]", 4)
    except ValueError:
        return
    assert False, "expected 'ValueError' exception was not raised"


class FakeK8SKluster:
    def __init__(self):
        self.get_api_client = mock.MagicMock()
        self.region_name = 'fake-region-1'


def get_k8s_endpoint_update(namespace, pod_name, ip, required_labels_in_place=True,
                            as_str=True):
    labels = {}
    if required_labels_in_place:
        labels = {k: "fake_value"
                  for k in (ScyllaPodsIPChangeTrackerThread.SCYLLA_PODS_EXPECTED_LABEL_KEYS)}
    as_dict = {
        # NOTE: every field that exists in real but makes no effect in our case
        #       is replaced with "fake_%its-key-name%" value.
        "type": "fake_type",
        "object": {
            "kind": "Endpoints",
            "apiVersion": "v1",
            "metadata": {
                "name": pod_name,
                "namespace": namespace,
                "uid": "fake_uid",
                "resourceVersion": "fake_resourceVersion",
                "creationTimestamp": "fake_creationTimestamp",
                "labels": labels,
                "annotations": "fake_annotations",
                "managedFields": "fake_managedFields"
            },
            "subsets": [{
                "addresses": [{
                    "ip": ip,
                    "hostname": "fake_hostname",
                    "nodeName": "fake_nodeName",
                    "targetRef": "fake_targetRef"
                }],
                "ports": "fake_ports"
            }]
        }
    }
    if not as_str:
        return as_dict
    as_str = str(as_dict).replace("\n", "")
    return as_str


ns1_p1_callbacks_as_list = [
    [mock.Mock(__name__="ns1_p1_callbacks_as_list[0]"), [mock.Mock()], {"fake_kwarg": mock.Mock()}],
    mock.Mock(__name__="ns1_p1_callbacks_as_list[1]"),
]
ns1_p2_callbacks_as_callable = mock.Mock(__name__="ns1_p2_callbacks_as_callable")
ns1_eachpod_callbacks_as_callable = mock.Mock(__name__="ns1_eachpod_callbacks_as_callable")
ns2_eachpod_callbacks_as_list = [
    mock.Mock(__name__="ns2_eachpod_callbacks_as_list[0]"),
    mock.Mock(__name__="ns2_eachpod_callbacks_as_list[1]"),
]


def test_scylla_pods_ip_change_tracker_01_positive_scenario():
    # Init objects
    core_v1_api_mock, k8s_kluster, ip_mapper = mock.Mock(), FakeK8SKluster(), {}
    namespace1, pod_names1 = 'scylla', ("pod-name-1", "pod-name-2", "pod-name-3")
    namespace2, pod_names2 = 'another-namespace', ("an-pod-name-1", "an-pod-name-2", "an-pod-name-3")
    fake_ip1, fake_ip2, fake_ip1a, fake_ip3, fake_ip3a = (
        'fake-ip-1', 'fake-ip-2', 'fake-ip-1a', 'fake-ip-3', 'fake-ip-3a')

    with mock.patch.object(KubernetesOps, "core_v1_api", side_effect=core_v1_api_mock):
        ip_tracker = ScyllaPodsIPChangeTrackerThread(k8s_kluster, ip_mapper)
        k8s_kluster.get_api_client.assert_called_once()
        core_v1_api_mock.assert_called_once()

    assert not ip_mapper, ip_mapper

    # Register callbacks
    ip_tracker.register_callbacks(
        callbacks=ns1_p1_callbacks_as_list,
        namespace=namespace1, pod_name=pod_names1[0], add_pod_name_as_kwarg=False)
    ip_tracker.register_callbacks(
        callbacks=ns1_p2_callbacks_as_callable,
        namespace=namespace1, pod_name=pod_names1[1], add_pod_name_as_kwarg=True)
    ip_tracker.register_callbacks(
        callbacks=ns1_eachpod_callbacks_as_callable,
        namespace=namespace1, pod_name='__each__', add_pod_name_as_kwarg=False)

    # Verify registered callbacks
    assert len(ip_mapper) == 1, ip_mapper

    ip_tracker.register_callbacks(
        callbacks=ns2_eachpod_callbacks_as_list, namespace=namespace2, add_pod_name_as_kwarg=True)

    assert len(ip_mapper) == 2, ip_mapper
    assert namespace1 in ip_mapper and namespace2 in ip_mapper, ip_mapper

    assert pod_names1[0] in ip_mapper[namespace1], ip_mapper
    assert 'callbacks' in ip_mapper[namespace1][pod_names1[0]], ip_mapper
    assert ip_mapper[namespace1][pod_names1[0]]['callbacks'] == [
        (ns1_p1_callbacks_as_list[0][0], ns1_p1_callbacks_as_list[0][1],
         ns1_p1_callbacks_as_list[0][2], False),
        (ns1_p1_callbacks_as_list[1], [], {}, False),
    ], ip_mapper
    assert pod_names1[1] in ip_mapper[namespace1], ip_mapper
    assert 'callbacks' in ip_mapper[namespace1][pod_names1[1]], ip_mapper
    assert ip_mapper[namespace1][pod_names1[1]]['callbacks'] == [
        (ns1_p2_callbacks_as_callable, [], {}, True),
    ], ip_mapper
    assert '__each__' in ip_mapper[namespace1], ip_mapper
    assert 'callbacks' in ip_mapper[namespace1]['__each__'], ip_mapper
    assert ip_mapper[namespace1]['__each__']['callbacks'] == [
        (ns1_eachpod_callbacks_as_callable, [], {}, False),
    ], ip_mapper
    assert ip_mapper[namespace2]['__each__']['callbacks'] == [
        (ns2_eachpod_callbacks_as_list[0], [], {}, True),
        (ns2_eachpod_callbacks_as_list[1], [], {}, True),
    ], ip_mapper

    # Process pods updates and assert it's data and callbacks call status
    ip_tracker._process_line(get_k8s_endpoint_update(namespace1, pod_names1[0], fake_ip1))
    assert pod_names1[0] in ip_mapper[namespace1], ip_mapper
    assert 'current_ip' in ip_mapper[namespace1][pod_names1[0]], ip_mapper
    assert fake_ip1 == ip_mapper[namespace1][pod_names1[0]]['current_ip'], ip_mapper
    assert ip_mapper[namespace1][pod_names1[0]]['old_ips'] == [], ip_mapper

    ip_tracker._process_line(get_k8s_endpoint_update(namespace1, pod_names1[1], fake_ip2))
    assert pod_names1[1] in ip_mapper[namespace1], ip_mapper
    assert 'current_ip' in ip_mapper[namespace1][pod_names1[1]], ip_mapper
    assert fake_ip2 == ip_mapper[namespace1][pod_names1[1]]['current_ip'], ip_mapper
    assert ip_mapper[namespace1][pod_names1[1]]['old_ips'] == [], ip_mapper

    ns1_p1_callbacks_as_list[0][0].assert_not_called()
    ns1_p1_callbacks_as_list[1].assert_not_called()
    ns1_p2_callbacks_as_callable.assert_not_called()
    ns1_eachpod_callbacks_as_callable.assert_not_called()
    ns2_eachpod_callbacks_as_list[0].assert_not_called()
    ns2_eachpod_callbacks_as_list[1].assert_not_called()

    ip_tracker._process_line(get_k8s_endpoint_update(namespace1, pod_names1[0], fake_ip1a))
    assert pod_names1[0] in ip_mapper[namespace1], ip_mapper
    assert 'current_ip' in ip_mapper[namespace1][pod_names1[0]], ip_mapper
    assert fake_ip1a == ip_mapper[namespace1][pod_names1[0]]['current_ip'], ip_mapper
    assert ip_mapper[namespace1][pod_names1[0]]['old_ips'] == [fake_ip1], ip_mapper

    ns1_p1_callbacks_as_list[0][0].assert_called_once_with(
        *ns1_p1_callbacks_as_list[0][1],
        **ns1_p1_callbacks_as_list[0][2])
    ns1_p1_callbacks_as_list[1].assert_called_once_with()
    ns1_p2_callbacks_as_callable.assert_not_called()
    ns1_eachpod_callbacks_as_callable.assert_called_once_with()
    ns2_eachpod_callbacks_as_list[0].assert_not_called()
    ns2_eachpod_callbacks_as_list[1].assert_not_called()

    ip_tracker._process_line(get_k8s_endpoint_update(namespace2, pod_names2[0], fake_ip3))
    assert pod_names2[0] in ip_mapper[namespace2], ip_mapper
    assert 'current_ip' in ip_mapper[namespace2][pod_names2[0]], ip_mapper
    assert fake_ip3 == ip_mapper[namespace2][pod_names2[0]]['current_ip'], ip_mapper
    assert ip_mapper[namespace2][pod_names2[0]]['old_ips'] == [], ip_mapper

    ip_tracker._process_line(get_k8s_endpoint_update(namespace2, pod_names2[0], fake_ip3a))
    assert fake_ip3a == ip_mapper[namespace2][pod_names2[0]]['current_ip'], ip_mapper
    assert ip_mapper[namespace2][pod_names2[0]]['old_ips'] == [fake_ip3], ip_mapper

    ns1_p1_callbacks_as_list[0][0].assert_called_once_with(
        *ns1_p1_callbacks_as_list[0][1],
        **ns1_p1_callbacks_as_list[0][2])
    ns1_p1_callbacks_as_list[1].assert_called_once_with()
    ns1_p2_callbacks_as_callable.assert_not_called()
    ns1_eachpod_callbacks_as_callable.assert_called_once_with()
    ns2_eachpod_callbacks_as_list[0].assert_called_once_with(pod_name=pod_names2[0])
    ns2_eachpod_callbacks_as_list[1].assert_called_once_with(pod_name=pod_names2[0])


def test_scylla_pods_ip_change_tracker_02_negative():
    core_v1_api_mock, k8s_kluster, ip_mapper = mock.Mock(), FakeK8SKluster(), {}

    with mock.patch.object(KubernetesOps, "core_v1_api", side_effect=core_v1_api_mock):
        ip_tracker = ScyllaPodsIPChangeTrackerThread(k8s_kluster, ip_mapper)
        k8s_kluster.get_api_client.assert_called_once()
        core_v1_api_mock.assert_called_once()

    ip_tracker._process_line(1)

    ip_tracker._process_line([])

    ip_tracker._process_line("")

    ip_tracker._process_line("some fake not parsible line")

    ip_tracker.register_callbacks(callbacks=[], namespace='fake', pod_name='fake')

    no_ns_dict = get_k8s_endpoint_update("fake-ns", "fake-pod-name", "fake-ip", as_str=False)
    no_ns_dict['object']['metadata'].pop('namespace')
    no_ns_str = str(no_ns_dict).replace("\n", "")
    ip_tracker._process_line(no_ns_str)

    assert not ip_mapper, ip_mapper
