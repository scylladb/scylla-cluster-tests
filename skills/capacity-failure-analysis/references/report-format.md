# Capacity Failure Report Format

Conventions for the HTML report. Applies whether the output is published as an Artifact or written to a
standalone file.

## Section Order

Lead with the grouping the reader asked for; the rest supports it.

1. **Header** — the headline number in a sentence: how many runs of how many, the single dominant signature,
   and where it concentrates
2. **Stat row** — failed runs, share of all runs, builds hit, regions affected, worst region by rate
3. **Primary grouping** — one row per region: failure count, rate meter, a 12-week strip, and the jobs hit
4. **Week x grouping** — stacked bar of failures per ISO week, coloured by region
5. **Table view** — the same week x region numbers as a table
6. **Region x job matrix** — which jobs are hit where
7. **Failure log** — every failure with Argus and Jenkins links, grouped by region
8. **Method and caveats** — detection rule, runs-vs-builds, region provenance, inference accuracy, scope

Sections 4 and 5 are a pair. The validated categorical palette has three light-mode hues below 3:1 contrast,
which obliges either visible direct labels or a table view; ship both.

## Numbers

- Always three together: runs, failures, rate. A bare count invites the wrong conclusion.
- Order regions by failure count in the spine, but discuss remediation by rate.
- Flag any region with fewer than ~10 runs as too small to rank.
- Use `font-variant-numeric: tabular-nums` everywhere digits line up.
- Give both runs and builds hit; they answer different questions.

## Colour

Use the validated categorical palette, slots in fixed order, one hue per region:

| Slot | Light | Dark |
|------|-------|------|
| 1 | `#2a78d6` | `#3987e5` |
| 2 | `#eb6834` | `#d95926` |
| 3 | `#1baf7a` | `#199e70` |
| 4 | `#eda100` | `#c98500` |
| 5 | `#e87ba4` | `#d55181` |

Regions with zero failures take a muted neutral, never a hue — spending a categorical slot on an unaffected
region implies it is part of the story. Keep page chrome near-monochrome so the region hues are the only
saturated element. Validate any substitution with the `dataviz` skill's `validate_palette.js`, and define
every colour as a token on bare `:root` so all three theme states resolve.

## Caveats Are Not Optional

The method section must carry:

- The exact detection rule, with the source file and line the error is raised at
- Runs versus builds, and which one each figure counts
- Region provenance: measured versus inferred, with the inference accuracy
- Scope: which tests were swept, and that the Argus CLI cannot enumerate a group so others may be missing

A capacity report drives scheduling decisions. Publishing region rankings without the provenance note
invites someone to move a job on the strength of an inferred value.

## Publishing

**Always write the report to `~/Downloads`, every run, without being asked.** Publishing as an Artifact as
well is good for sharing a link, but the local copy is not optional: an Artifact lives behind a URL and a
session, while the file on disk is what survives, gets attached to a ticket, and can be reopened offline.

Name it `capacity-failures-<period>-<YYYY-MM-DD>.html`, for example
`capacity-failures-month-2026-09-10.html`. The period is in the name because reports for different windows
are not interchangeable — a `week` report and an `all` report of the same date carry very different region
confidence, and a name without the period invites someone to quote the wrong one. Regenerating the same
period on the same day overwrites deliberately; a different period never collides.

The local file needs a real skeleton, because the Artifact host supplies one only at publish time and the
artifact source alone opens in quirks mode:

```html
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  html{color-scheme:light dark}
  body{margin:0;font:14px system-ui,sans-serif}
  img{max-width:100%}
  [hidden]{display:none!important}
</style>
</head>
<body>
<!-- report content -->
</body>
</html>
```

Report the saved path to the user alongside any Artifact link.
