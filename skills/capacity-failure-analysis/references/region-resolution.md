# Resolving the Region of a Capacity Failure

The hardest part of this analysis. Argus knows the region for runs that provisioned successfully and knows
nothing for exactly the runs being measured.

## Why Argus Has No Region

A capacity failure aborts the test before any instance is allocated, so the fields that would carry the
region are never populated:

| Field | Passed run | Capacity-failed run |
|-------|-----------|---------------------|
| `region_name` | `["eu-west-2"]` | `[]` |
| `allocated_resources` | Full node list with per-node `region` | `[]` |
| `sct_runner_host` | Object with IPs | `null` |
| `cloud_setup` | Instance types and AMIs | `null` |

Measured on a 12-week sweep: **0 of 77** capacity-failed runs had any region in Argus, against 381 of 471
runs overall. Never assume the field will be there — check, and fall back.

## Source 1: Jenkins Build Parameter (Authoritative)

The `region` build parameter of the Jenkins build that launched the run:

```python
url = build_job_url.rstrip("/") + "/api/json?tree=number,timestamp,result,actions[parameters[name,value]]"
response = requests.get(url, auth=(creds["username"], creds["password"]), timeout=90)
```

Credentials come from `KeyStore().get_json("jenkins.json")` — the same shared account SCT tooling uses
elsewhere. Requires `PYTHONPATH=.` when running a script outside `sct.py`.

**Validation:** across a full dataset, wherever both the Jenkins parameter and an Argus region existed they
agreed on **207 of 207** runs. The parameter can be trusted where it is available.

**Coverage is the problem.** Jenkins rotates old builds out of history and they answer HTTP 404. Coverage
falls off sharply with window length, which is exactly why the skill asks the user to pick a period before
collecting anything:

| Period | Flag | Typical Jenkins coverage | Consequence |
|--------|------|--------------------------|-------------|
| Last week | `--period week` | Near total | Region breakdown is essentially all measured |
| Last month | `--period month` | High | A small inferred tail, safe to rank regions |
| From first start | `--period all` | Low and falling with age | Most regions inferred; report shape, not rankings |

These are expectations, not guarantees — the rotation depth changes per job. Always report the actual
coverage the run produced rather than quoting this table.

Note the pipeline definitions under `jenkins-pipelines/performance/` do **not** set a region; it comes from
the job's parameter defaults or the trigger, so it can only be read per build, never from the repository.

## Source 2: Nearest-Neighbour Inference (Fallback)

For runs whose build has rotated away, take the region of the temporally closest run of the **same job**
that has a known region. Jobs stay in one region for long stretches, so this is usually right.

**Always cross-validate before trusting it.** Predict the region of runs whose region *is* known and measure
the hit rate:

```
nearest-neighbour cross-validation: 391 correct / 427 (91.6%)
```

The helper script prints this automatically. Rules:

- Above ~90%: usable, provided every inferred value is labelled
- 70-90%: usable for the overall shape, not for ranking small regions
- Below 70%: do not infer; report the measured subset only and say what is missing

Never infer across different jobs. Two jobs running the same week can sit in different regions, so a
cross-job neighbour carries no signal.

## Reporting Rules

State the split every time, for example:

> Region comes from the Jenkins `region` build parameter (39 of 77). The remaining 38 failures sit on builds
> that have rotated out of Jenkins history, so their region is inferred from the nearest run of the same job,
> an inference that cross-validates at 91.6%.

Mark inferred values visibly in any table or report, and warn that small regions are the ones most sensitive
to inference error — a region with 3 failures can move materially on a single wrong guess.

## Fallback Order

1. Jenkins `region` build parameter — measured, authoritative
2. Argus `region_name` or `allocated_resources[].instance_info.region` — measured, but absent on failures
3. Nearest run of the same job — inferred, must be cross-validated and labelled
4. Otherwise `unknown` — report as its own bucket, never silently drop
