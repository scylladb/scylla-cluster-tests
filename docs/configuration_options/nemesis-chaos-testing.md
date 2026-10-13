# Nemesis (chaos testing)

[← All configuration options](../configuration_options.md)

Which disruptions run, how often, and how targets are selected.

**13 options.**


<a id="nemesis"></a>

## **nemesis** / SCT_NEMESIS

Nemesis configuration: nemesis-wide settings, defaults shared by all runners, and the<br>runners to start, one thread each. `runners` is a list of single-key mappings of a<br>runner class to its options, e.g. `{SisyphusMonkey: {selector: disruptive, interval: 15}}`;<br>the options accepted depend on the runner class. Options a runner leaves unset take<br>the [`nemesis`](#nemesis) defaults; a runner without `seed` gets a random one. `selector` is a<br>logical expression over nemesis flags filtering the disruptions a runner runs.<br>Accepts YAML or JSON from the environment, for the whole option or one sub-key, e.g.<br>SCT_NEMESIS__INTERVAL=10 or SCT_NEMESIS__RUNNERS='[{"SisyphusMonkey": {"selector": "disruptive"}}]'.

**default:** {'runners': [{'NoOpMonkey': {}}]}

**type:** sdcm.sct_config.nemesis_runners.NemesisConfig

**sub-options:**

| option | type | default | description |
|---|---|---|---|
| `during_prepare` | bool | True | Run nemesis during the prepare stage of the test |
| `sequence_sleep_between_ops` | int | N/A | Minutes to sleep between the operations of sequence nemeses |
| `grow_shrink.add_node_cnt` | int | 3 | Number of nodes added and removed |
| `grow_shrink.instance_type` | str | N/A | Instance type of the added nodes; the cluster's DB instance type when unset |
| `grow_shrink.double_load_duration` | int | 0 | Minutes to double the load for after growing and before shrinking; 0 disables it |
| `interval` | int | 5 | Default for runner `interval` |
| `seed` | int | N/A | Default for runner `seed`; a random one when unset |
| `filter_seeds` | bool | False | Default for runner `filter_seeds` |
| `selector` | str | N/A | Default for runner `selector` |
| `multiply_factor` | int | 2 | Default for runner `multiply_factor` |
| `runners` | list[...] | [] | Runners to start, one thread each |


<a id="nemesis_add_node_cnt"></a>

## **nemesis_add_node_cnt** / SCT_NEMESIS_ADD_NODE_CNT

Not read; replaced by `nemesis.grow_shrink.add_node_cnt`.

**default:** 3

**type:** int


<a id="nemesis_class_name"></a>

## **nemesis_class_name** / SCT_NEMESIS_CLASS_NAME

Not read; replaced by `nemesis.runners`.

**default:** NoOpMonkey

**type:** str | list[str] → list[str] (appendable)


<a id="nemesis_double_load_during_grow_shrink_duration"></a>

## **nemesis_double_load_during_grow_shrink_duration** / SCT_NEMESIS_DOUBLE_LOAD_DURING_GROW_SHRINK_DURATION

Not read; replaced by `nemesis.grow_shrink.double_load_duration`.

**default:** 0

**type:** int


<a id="nemesis_during_prepare"></a>

## **nemesis_during_prepare** / SCT_NEMESIS_DURING_PREPARE

Not read; replaced by `nemesis.during_prepare`.

**default:** True

**type:** bool


<a id="nemesis_filter_seeds"></a>

## **nemesis_filter_seeds** / SCT_NEMESIS_FILTER_SEEDS

Not read; replaced by `nemesis.filter_seeds`.

**default:** False

**type:** bool


<a id="nemesis_grow_shrink_instance_type"></a>

## **nemesis_grow_shrink_instance_type** / SCT_NEMESIS_GROW_SHRINK_INSTANCE_TYPE

Not read; replaced by `nemesis.grow_shrink.instance_type`.

**default:** N/A

**type:** str (appendable)


<a id="nemesis_interval"></a>

## **nemesis_interval** / SCT_NEMESIS_INTERVAL

Not read; replaced by `nemesis.interval`.

**default:** 5

**type:** int


<a id="nemesis_multiply_factor"></a>

## **nemesis_multiply_factor** / SCT_NEMESIS_MULTIPLY_FACTOR

Not read; replaced by `nemesis.multiply_factor`.

**default:** 2

**type:** int


<a id="nemesis_seed"></a>

## **nemesis_seed** / SCT_NEMESIS_SEED

Not read; replaced by `nemesis.seed`.

**default:** N/A

**type:** int | list[int] → list[int]


<a id="nemesis_selector"></a>

## **nemesis_selector** / SCT_NEMESIS_SELECTOR

Not read; replaced by `nemesis.selector`.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="nemesis_sequence_sleep_between_ops"></a>

## **nemesis_sequence_sleep_between_ops** / SCT_NEMESIS_SEQUENCE_SLEEP_BETWEEN_OPS

Not read; replaced by `nemesis.sequence_sleep_between_ops`.

**default:** N/A

**type:** int


<a id="sla"></a>

## **sla** / SCT_SLA

run SLA nemeses if the test is SLA only

**default:** N/A

**type:** bool
