# Nemesis (chaos testing)

[← All configuration options](../configuration_options.md)

Which disruptions run, how often, and how targets are selected.

**13 options.**


<a id="nemesis"></a>

## **nemesis** / SCT_NEMESIS

Nemesis configuration: nemesis-wide settings, defaults shared by all runners, and the<br>runners to start, one thread each. Each runner is a single-key mapping of the runner<br>class to its options; the options accepted depend on the runner class. Options a<br>runner leaves unset take the [`nemesis`](#nemesis) defaults; a runner without `seed` gets a<br>random one. `selector` is a logical expression over nemesis flags filtering the<br>disruptions a runner runs.<br>YAML example:<br>nemesis:<br>interval: 5<br>during_prepare: false<br>grow_shrink:<br>[`add_node_cnt`](general-and-provisioning.md#add_node_cnt): 3<br>instance_type: i4i.xlarge<br>runners:<br>- SisyphusMonkey:<br>selector: "disruptive"<br>interval: 15<br>- CategoricalMonkey:<br>dist: {StopStartMonkey: 2}<br>Environment variable examples:<br>SCT_NEMESIS__INTERVAL=10<br>SCT_NEMESIS__GROW_SHRINK='{"[`add_node_cnt`](general-and-provisioning.md#add_node_cnt)": 5}'<br>SCT_NEMESIS__RUNNERS='[{"SisyphusMonkey": {"selector": "disruptive"}}]'

**default:** {'runners': [{'NoOpMonkey': {}}]}

**type:** sdcm.sct_config.nemesis_runners.NemesisConfig


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
