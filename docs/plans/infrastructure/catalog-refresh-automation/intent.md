# Automate the instance catalog refresh

**Jira:** SCT-852

## Problem

`data/instance_catalog/` holds the prices that every cost estimate uses.
Nothing refreshes it on a schedule. It changes only when a person runs
`sct sizing update-catalog` by hand, and in practice that happens only when
something else goes wrong.

The prices move. Spot prices move most. A catalog that nobody refreshes gives
cost estimates from old spot prices.

A refresh can also damage the catalog. The generators catch their errors and
return the instance types that they collected before the error. The refresh
skips the file write only when a generator returns nothing. Thus a
rate-limited pricing API or a changed pricing page writes a smaller catalog,
and the diff looks like a normal price update. With a person at the
keyboard, somebody can see this. A scheduled refresh has nobody to see it.

## Who it affects

- Each cost estimate and each sizing decision that reads the catalog, on AWS,
  GCE, Azure, and OCI.
- The maintainers of the catalog, who must remember to refresh it.

## Evidence

- Azure spot rows have effective dates on month boundaries. About one third
  of the SKUs changed their spot price in three months.
- GCE spot showed no change across 3,707 (type, region) prices in eight days,
  but the Google documentation lets the price change every day.
- On-demand prices drift slowly. The median Azure on-demand row is 540 days
  old. Spot is what makes an old catalog wrong.
- The catalog history on master holds only hand-made commits: new families,
  capacity fixes, and removals. No commit is a scheduled price refresh.
- The generator module catches errors in many places and returns a partial
  result. The write step checks only for an empty result.

## Definition of done

- The catalog refresh runs on a schedule of about two months, without a
  person who remembers it.
- A person can start a refresh by hand at any time.
- A refresh that changes nothing proposes nothing.
- A refresh that changes prices proposes the change as a pull request. A
  person reviews it and merges it. This is the only manual step.
- The pull request says what moved, per cloud: how many instance types
  changed, the largest price changes, and the instance types that went away.
  A reviewer does not have to read thousands of YAML lines.
- A refresh that loses a large share of the instance types of a cloud fails
  and writes nothing. A person can let a real removal through, but only on
  purpose.
- A failed scheduled refresh is visible to the owners. It does not wait
  silently for the next run.
- A person can see what a refresh would change, without a write.

## Out of scope

- What the catalog contains, and how the generators derive each price.
- Alerts on price changes. The review of the pull request covers them.
- Precision. The catalog is an estimate, and a two-month refresh is accurate
  enough.
