# Nemesis (chaos testing)

[← All configuration options](../configuration_options.md)

Which disruptions run, how often, and how targets are selected.

**13 options.**


<a id="nemesis"></a>

## **nemesis** / SCT_NEMESIS

Nemesis runners to start, one thread each, and defaults shared by all of them.<br>Each runner is a single-key mapping of the runner class to its options; the options<br>accepted depend on the runner class. Options a runner leaves unset take the<br>[`nemesis`](#nemesis) defaults; a runner without `seed` gets a random one. `selector` is a<br>logical expression over nemesis flags filtering the disruptions a runner runs.<br>YAML example:<br>nemesis:<br>interval: 5<br>runners:<br>- SisyphusMonkey:<br>selector: "disruptive"<br>interval: 15<br>- CategoricalMonkey:<br>dist: {StopStartMonkey: 2}<br>Environment variable examples:<br>SCT_NEMESIS__INTERVAL=10<br>SCT_NEMESIS__RUNNERS='[{"SisyphusMonkey": {"selector": "disruptive"}}]'

**default:** {'runners': [{'NoOpMonkey': {}}]}

**type:** sdcm.sct_config.nemesis_runners.NemesisConfig


<a id="nemesis_add_node_cnt"></a>

## **nemesis_add_node_cnt** / SCT_NEMESIS_ADD_NODE_CNT

Add/remove nodes during GrowShrinkCluster nemesis

**default:** 3

**type:** int


<a id="nemesis_class_name"></a>

## **nemesis_class_name** / SCT_NEMESIS_CLASS_NAME

Not read; replaced by `nemesis.runners`.

**default:** NoOpMonkey

**type:** str | list[str] → list[str] (appendable)


<a id="nemesis_double_load_during_grow_shrink_duration"></a>

## **nemesis_double_load_during_grow_shrink_duration** / SCT_NEMESIS_DOUBLE_LOAD_DURING_GROW_SHRINK_DURATION

After growing (and before shrink) in GrowShrinkCluster nemesis it will double the load for provided duration.

**default:** 0

**type:** int


<a id="nemesis_during_prepare"></a>

## **nemesis_during_prepare** / SCT_NEMESIS_DURING_PREPARE

Run nemesis during prepare stage of the test

**default:** True

**type:** bool


<a id="nemesis_filter_seeds"></a>

## **nemesis_filter_seeds** / SCT_NEMESIS_FILTER_SEEDS

Not read; replaced by `nemesis.filter_seeds`.

**default:** False

**type:** bool


<a id="nemesis_grow_shrink_instance_type"></a>

## **nemesis_grow_shrink_instance_type** / SCT_NEMESIS_GROW_SHRINK_INSTANCE_TYPE

Instance type to use for adding/removing nodes during GrowShrinkCluster nemesis

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

Sleep interval between nemesis operations for use in unique_sequence nemesis kind of tests

**default:** N/A

**type:** int


<a id="sla"></a>

## **sla** / SCT_SLA

run SLA nemeses if the test is SLA only

**default:** N/A

**type:** bool
