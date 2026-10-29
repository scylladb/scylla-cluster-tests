## Configuration Options

### A Default for Each Option

Each option in `sdcm/sct_config/` has a default value in
`defaults/test_default.yaml`, or in the backend file in `defaults/`, as in
`aws_config.yaml`. An option without a default reads as `None` in the tests
that do not set it.

### Describe the Option

Declare the option with `SctField` and a `description`. Give its type, its
default, its valid range, and an example. The `update-conf-docs` pre-commit
hook then updates `docs/configuration_options.md` and the group pages in
`docs/configuration_options/`.

### Pinned Settings and Nemesis

A nemesis can rewrite a setting that a test case pins, as with the compaction
strategy, the compression, or a `scylla.yaml` option. Exclude that nemesis by
name in `nemesis_selector`. Otherwise the nemesis undoes the setting during
the run.

Sources: `.github/copilot-instructions.md` section Configuration Changes,
`AGENTS.md` sections Test Configuration and Documentation Standards,
`skills/code-review/SKILL.md` check 6, `docs/nemesis.md`.
