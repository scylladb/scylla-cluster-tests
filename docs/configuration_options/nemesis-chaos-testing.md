# Nemesis (chaos testing)

[← All configuration options](configuration_options.md)

Which disruptions run, how often, and how targets are selected.

**12 options.**


## **nemesis_add_node_cnt** / SCT_NEMESIS_ADD_NODE_CNT

Add/remove nodes during GrowShrinkCluster nemesis

**default:** 3

**type:** int


## **nemesis_class_name** / SCT_NEMESIS_CLASS_NAME

Nemesis class to use (possible types in sdcm.nemesis).<br>Supported syntax:<br>- [`nemesis_class_name`](#nemesis_class_name): "NemesisName"<br>Run one nemesis in a single thread.<br>- [`nemesis_class_name`](#nemesis_class_name): ["NemesisA", "NemesisB"]<br>Run NemesisA and NemesisB each in their own thread.<br>- [`nemesis_class_name`](#nemesis_class_name): ["SisyphusMonkey", "SisyphusMonkey"]<br>Run two SisyphusMonkey threads in parallel.<br>Note: the former 'Class:N' count syntax (e.g. "ChaosMonkey:2") and<br>space-separated strings (e.g. "DisruptiveMonkey NonDisruptiveMonkey") are no<br>longer supported. Use an explicit YAML list instead.

**default:** NoOpMonkey

**type:** str | list[str] → list[str] (appendable)


## **nemesis_double_load_during_grow_shrink_duration** / SCT_NEMESIS_DOUBLE_LOAD_DURING_GROW_SHRINK_DURATION

After growing (and before shrink) in GrowShrinkCluster nemesis it will double the load for provided duration.

**default:** 0

**type:** int


## **nemesis_during_prepare** / SCT_NEMESIS_DURING_PREPARE

Run nemesis during prepare stage of the test

**default:** True

**type:** bool


## **nemesis_filter_seeds** / SCT_NEMESIS_FILTER_SEEDS

If true runs the nemesis only on non seed nodes

**default:** False

**type:** bool


## **nemesis_grow_shrink_instance_type** / SCT_NEMESIS_GROW_SHRINK_INSTANCE_TYPE

Instance type to use for adding/removing nodes during GrowShrinkCluster nemesis

**default:** N/A

**type:** str (appendable)


## **nemesis_interval** / SCT_NEMESIS_INTERVAL

Nemesis sleep interval to use if None provided specifically in the test

**default:** 5

**type:** int


## **nemesis_multiply_factor** / SCT_NEMESIS_MULTIPLY_FACTOR

Multiply the list of nemesis to execute by the specified factor

**default:** 2

**type:** int


## **nemesis_seed** / SCT_NEMESIS_SEED

A seed number in order to repeat nemesis sequence as part of SisyphusMonkey

**default:** N/A

**type:** int | list[int] | space-separated ints → list[int]


## **nemesis_selector** / SCT_NEMESIS_SELECTOR

[`nemesis_selector`](#nemesis_selector) gets a list of "nemesis properties" and filters IN all the nemesis that has<br>ALL the properties in that list which are set to true (the intersection of all properties).<br>(In other words filters out all nemesis that doesn't ONE of these properties set to true)<br>IMPORTANT: If a property doesn't exist, ALL the nemesis will be included.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **nemesis_sequence_sleep_between_ops** / SCT_NEMESIS_SEQUENCE_SLEEP_BETWEEN_OPS

Sleep interval between nemesis operations for use in unique_sequence nemesis kind of tests

**default:** N/A

**type:** int


## **sla** / SCT_SLA

run SLA nemeses if the test is SLA only

**default:** N/A

**type:** bool
