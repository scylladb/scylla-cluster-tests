from sdcm.utils.k8s import HelmValues


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


def test_helm_values_try_get_by_list_index():
    helm_values = HelmValues(BASE_HELM_VALUES)
    try:
        helm_values.get("nested_list[0]")
    except ValueError:
        return
    assert False, "expected 'ValueError' exception was not raised"


def test_helm_values_try_set_by_list_index():
    helm_values = HelmValues(BASE_HELM_VALUES)
    try:
        helm_values.set("nested_list[0]", 4)
    except ValueError:
        return
    assert False, "expected 'ValueError' exception was not raised"
