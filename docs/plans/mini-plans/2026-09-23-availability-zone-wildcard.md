# Mini-Plan: `availability_zone: '*'` — let the backend pick the AZ

**Date:** 2026-09-23
**Estimated LOC:** ~600
**Related PR:** follow-up to #15839 (spot placement scoring)

## Problem

Every test case and backend default has to name specific availability zones, and almost none of them
mean it. `defaults/aws_config.yaml` pins `'a'` and `defaults/oci_config.yaml` pins `'a'` for every run;
`defaults/k8s_gke_config.yaml` pins `'c'`. Those letters are not a requirement of the test — they are a
value someone had to write because the field cannot be left to the backend.

This costs us three ways:

1. **Capacity signals cannot act.** #15839 ranks AZs by spot placement score (AWS) and Capacity Advisor
   obtainability (GCE), but a named AZ outranks the score by design. That PR works around it by treating a
   `defaults/`-sourced value as "not a real choice" — a provenance heuristic that exists only because there
   is no way to write "I don't care which AZ".
2. **Rack count and AZ identity are tangled.** `racks_count` is derived from how many comma-separated
   entries `availability_zone` has, so asking for three racks means naming three specific zones — even when
   any three would do.
3. **Backends disagree.** `sdcm/provision/gce/zone_resolver.py:GceAZResolver` already picks a zone when none
   is configured; `sdcm/provision/aws/az_resolver.py:AZResolver` leaves the value empty instead; Azure and
   OCI have no resolver at all. The same config means different things per backend.

## Approach

Introduce `*` as an availability-zone entry meaning "one AZ, chosen by the backend". It is a placeholder in
the existing comma-separated list, so `'*,*,*'` asks for three distinct AZs without naming them, and
`'a,*'` pins one and leaves the second free. The count keeps driving `racks_count` exactly as today.

End to end: `availability_zone` parsed → wildcard slots counted → backend lists AZs valid for the required
instance types → candidates ranked (spot score / obtainability where available, else stable order) →
wildcards filled from the ranked list, skipping explicitly named AZs → resolved value written back to
config → persisted to the resolved-placement handoff so a later `run-test` step reuses it.

**Resolve once, early, and persist every time.** A wildcard is resolved at the first step of a job that
needs a concrete AZ, and the result is written to the resolved-placement handoff *unconditionally* —
today the handoff is written only when AWS relocates to another region, and Azure and OCI write nothing,
so a later step would re-resolve `*` and could land in a different AZ. Every later step (including
`run-test` after runner creation) reads the handoff first and treats the stored value as the configured
one. Code that reads `availability_zone` after that point therefore sees a concrete zone and needs no
wildcard awareness of its own.

Steps, in order:

- Extend AZ parsing (`sdcm/utils/cloud_api_utils.py`) to recognise `*` as a distinct token rather than a
  zone letter, and expose how many wildcard slots a value carries. Validation rejects a value that is only
  wildcards on a backend that cannot enumerate its zones.
- Give AWS the resolution behaviour GCE already has: when a slot is a wildcard, fill it from the ranked
  candidate list rather than leaving the value untouched. Reuse the existing offerings filter as the hard
  constraint and the placement score as the ordering, so a wildcard AZ is chosen on capacity, not
  alphabetically. Align the shortfall behaviour with GCE: when fewer valid AZs exist than slots requested,
  AWS raises instead of silently returning a shorter list, which would otherwise shrink the rack count.
- Apply the same to GCE, where the picking logic exists but is currently reachable only when the whole
  field is empty — a wildcard must go through the same path as "unset", and a partly-wildcard value must
  keep its named entries.
- Add minimal zone resolution for Azure and OCI: enumerate the region's zones, keep the backend's existing
  ordering, and fill wildcard slots from it. No capacity signal exists on these backends, so the goal is
  correctness and parity, not optimisation. Wire both into the resolved-placement handoff, which they do
  not use today — this is also the groundwork region/AZ fallback on these backends will need.
- Resolve the SCT runner's AZ through the same path. The runner is created by its own Jenkins step before
  any test-side resolver runs, and both the pipeline and the AWS/GCE runner classes take the first
  comma-separated entry as a literal zone letter — a raw `*` would pass the one-letter check and then
  build an invalid `<region>*` zone name. Runner creation becomes the first resolution point when a job
  has a runner, and it writes the handoff that `run-test` then reuses.
- Reject `*` in config validation for every consumer that reads `availability_zone` without going through
  a resolver and runs before (or outside) the resolution point: EMR, xcloud AWS and GCE loaders, GKE, EKS,
  and AWS dedicated hosts (which read the AZ at config load). Each can later opt in by resolving the
  wildcard; until then a clear validation error beats a zone named `*`.
- Once every backend and the runner resolve wildcards, change `defaults/aws_config.yaml` and
  `defaults/oci_config.yaml` to `'*'` and retire the provenance heuristic added in #15839, which exists only to work around the absence
  of this feature.
- Update the `availability_zone` option description and regenerate the configuration docs.

**Separator.** The wildcard uses the existing comma separator (`'*,*,*'`), not spaces: `availability_zone`
is parsed comma-separated everywhere, while space separates *regions* in `region_name`. A space-separated
`'* * *'` would parse as one zone literally named `* * *`. If a shorthand is wanted later, `'*3'` is
unambiguous; that is deliberately out of scope here.

**`hydra output-conf` is not a resolution point.** It builds the configuration without calling cloud
APIs, so it keeps printing `*`. Its only pipeline consumer (`vars/getJobTimeouts.groovy`) reads test
durations, not placement, so an unresolved AZ there is harmless. Resolver behaviour is verified with unit
tests instead.

**Needs investigation:** whether multi-region configs can mix wildcards per region, or whether a wildcard
must resolve to the same letter in every region. The AWS resolver requires an AZ letter to be valid in all
configured regions, so `'*'` there means "a letter available everywhere", which is a narrower guarantee than
it looks. Resolve this before implementing the multi-region case; single-region is unaffected.

## Files to Modify

- `sdcm/utils/cloud_api_utils.py` — wildcard-aware AZ parsing and slot counting
- `sdcm/provision/aws/az_resolver.py` — fill wildcard slots from the score-ranked candidates
- `sdcm/provision/gce/zone_resolver.py` — route wildcards through the existing picking path
- `sdcm/provision/azure/provisioner.py` — resolve wildcard slots from the region's zones
- `sdcm/provision/oci/provisioner.py` — same for OCI
- `sdcm/test_config.py` — persist the resolved AZ on every resolution, for all backends
- `sdcm/sct_runner.py`, `vars/createSctRunner.groovy` — resolve a wildcard for the runner and write the handoff
- `sdcm/tester.py` — read the handoff before provisioning on every backend
- `sdcm/sct_config/config.py` — validation (including rejecting `*` for EMR, xcloud, k8s and dedicated hosts), and keep `racks_count` derivation working for wildcards
- `sdcm/sct_config/mixins/common.py` — `availability_zone` description
- `defaults/aws_config.yaml`, `defaults/oci_config.yaml` — switch the pinned `'a'` to `'*'` (last step)
- `docs/configuration_options/` — regenerated
- `unit_tests/unit/provisioner/test_az_resolver.py`, `unit_tests/unit/provisioner/test_gce_zone_resolver.py` — wildcard cases
- `unit_tests/unit/provisioner/test_az_wildcard.py` *(new)* — shared parsing, validation, and per-backend resolution

## Verification

- [ ] `'*'` on AWS resolves to a real AZ letter, and to the best-scoring one when spot scoring is enabled
- [ ] `'*,*,*'` resolves to three *distinct* AZs, and `racks_count` is 3
- [ ] `'a,*'` keeps `a` and fills only the second slot
- [ ] A named AZ still wins over the score, i.e. the wildcard is the only way to delegate the choice
- [ ] Every backend resolves `'*'`: unit tests for AWS, GCE, Azure and OCI show a concrete AZ
- [ ] A wildcard that cannot be resolved (no zone supports the instance types, or fewer valid zones than
      slots) fails with a clear error on every backend, AWS included, rather than returning fewer AZs
- [ ] The resolved AZ reaches the placement handoff on every backend, so a later `run-test` uses the same
      zone even when no relocation happened
- [ ] The SCT runner created with `'*'` lands in the same AZ the test then provisions in
- [ ] `'*'` with EMR, xcloud, GKE, EKS or dedicated hosts fails config validation with a clear message
- [ ] Unit tests: `uv run pytest unit_tests/unit/provisioner/ -v`
- [ ] A short longevity provisions on AWS, GCE, Azure and OCI with `availability_zone: '*'`
- [ ] `uv run sct.py pre-commit` passes
