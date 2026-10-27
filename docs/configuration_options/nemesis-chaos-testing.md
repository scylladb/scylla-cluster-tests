# Nemesis (chaos testing)

[← All configuration options](../configuration_options.md)

Which disruptions run, how often, and how targets are selected. Every option lives in the
[`nemesis`](#nemesis) section; see `sdcm.sct_config.sections.nemesis`.

**14 options.**


<a id="nemesis"></a>

## **nemesis** / SCT_NEMESIS

Nemesis section: settings shared by all runners, defaults for options a runner leaves<br>unset, and `runners`, the runners to start, one thread each. A runner entry is a<br>single-key mapping of a runner class to its options, e.g.<br>`{SisyphusMonkey: {selector: disruptive, interval: 15}}`; the options accepted depend<br>on the runner class. A runner without `seed`, own or default, gets a random one.

**default:** the defaults of its options

**type:** section (10 options below)


<a id="nemesis.during_prepare"></a>

## **nemesis.during_prepare** / SCT_NEMESIS__DURING_PREPARE

Run nemesis during the prepare stage of the test

**default:** True

**type:** bool

```yaml
nemesis:
  during_prepare: True
```


<a id="nemesis.sequence_sleep_between_ops"></a>

## **nemesis.sequence_sleep_between_ops** / SCT_NEMESIS__SEQUENCE_SLEEP_BETWEEN_OPS

Minutes to sleep between the operations of sequence nemeses

**default:** N/A

**type:** int

```yaml
nemesis:
  sequence_sleep_between_ops: ...
```


<a id="nemesis.sla"></a>

## **nemesis.sla** / SCT_NEMESIS__SLA

Run the SLA nemeses; set only in SLA tests

**default:** N/A

**type:** bool

```yaml
nemesis:
  sla: ...
```


<a id="nemesis.grow_shrink"></a>

## **nemesis.grow_shrink** / SCT_NEMESIS__GROW_SHRINK

Options of the nemeses that grow and shrink the cluster

**default:** the defaults of its options

**type:** section (3 options below)


<a id="nemesis.grow_shrink.add_node_cnt"></a>

## **nemesis.grow_shrink.add_node_cnt**

Number of nodes added and removed

**default:** 3

**type:** int

**env:** within the YAML/JSON of `SCT_NEMESIS__GROW_SHRINK`

```yaml
nemesis:
  grow_shrink:
    add_node_cnt: 3
```


<a id="nemesis.grow_shrink.instance_type"></a>

## **nemesis.grow_shrink.instance_type**

Instance type of the added nodes; the cluster's DB instance type when unset

**default:** N/A

**type:** str (appendable)

**env:** within the YAML/JSON of `SCT_NEMESIS__GROW_SHRINK`

```yaml
nemesis:
  grow_shrink:
    instance_type: ...
```


<a id="nemesis.grow_shrink.double_load_duration"></a>

## **nemesis.grow_shrink.double_load_duration**

Minutes to double the load for after growing and before shrinking; 0 disables it

**default:** 0

**type:** int

**env:** within the YAML/JSON of `SCT_NEMESIS__GROW_SHRINK`

```yaml
nemesis:
  grow_shrink:
    double_load_duration: 0
```


<a id="nemesis.interval"></a>

## **nemesis.interval** / SCT_NEMESIS__INTERVAL

Default for runner `interval`: minutes between disruptions

**default:** 5

**type:** int

```yaml
nemesis:
  interval: 5
```


<a id="nemesis.seed"></a>

## **nemesis.seed** / SCT_NEMESIS__SEED

Default for runner `seed`; a random one per runner when unset

**default:** N/A

**type:** int

```yaml
nemesis:
  seed: ...
```


<a id="nemesis.filter_seeds"></a>

## **nemesis.filter_seeds** / SCT_NEMESIS__FILTER_SEEDS

Default for runner `filter_seeds`: target non-seed nodes only

**default:** False

**type:** bool

```yaml
nemesis:
  filter_seeds: False
```


<a id="nemesis.selector"></a>

## **nemesis.selector** / SCT_NEMESIS__SELECTOR

Default for runner `selector`: a logical expression over nemesis flags filtering the disruptions

**default:** N/A

**type:** str (appendable)

```yaml
nemesis:
  selector: ...
```


<a id="nemesis.multiply_factor"></a>

## **nemesis.multiply_factor** / SCT_NEMESIS__MULTIPLY_FACTOR

Default for runner `multiply_factor`: how many times the disruption list repeats before shuffling

**default:** 2

**type:** int

```yaml
nemesis:
  multiply_factor: 2
```


<a id="nemesis.runners"></a>

## **nemesis.runners** / SCT_NEMESIS__RUNNERS

Runners to start, one thread each. Every runner takes `interval`, `seed` and<br>`filter_seeds`; most also take `selector` and `multiply_factor`; a runner with options of<br>its own (e.g. CategoricalMonkey `dist`, FixedOrderMonkey `order`) declares them in its<br>config model. See docs/nemesis.md.

**default:** [{'NoOpMonkey': {}}]

**type:** list of {<RunnerClass>: {<options>}} | YAML/JSON string (appendable)

```yaml
nemesis:
  runners: [{'NoOpMonkey': {}}]
```
