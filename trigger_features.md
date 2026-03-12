# suggestions for staging_trigger

## tab completion of job names from jenkins, and of filenames/folders

## ~~list of suggestion for some parameters~~ (DONE)

Implemented in `utils/staging_trigger/interactive.py`:
- `scylla_version` parameter now shows a `questionary.select()` with suggestions:
  - `master:latest`, latest release from `get_latest_scylla_release()`, current value, and "Custom value..."
- Jenkins choice parameters (e.g. `provision_type`, `post_behavior_*`) are detected from job config XML
  and presented as `questionary.select()` with the available choices + "Custom value..." fallback
- Other parameters still use `questionary.text()` as before
- `ParamDefinition` dataclass and `parse_parameter_definitions()` added to parse Jenkins XML metadata

## ~~better preset for docker~~ (DONE)

Implemented in `utils/staging_trigger/interactive.py` and `utils/staging_trigger/trigger.py`:
- After editing parameters interactively, `maybe_set_docker_image()` auto-derives `scylla_docker_image`
  from `scylla_version` using `get_scylla_docker_repo_from_version()` when backend is `docker`
- For non-interactive paths (YAML config, direct CLI), `_auto_set_docker_image()` in `trigger.py`
  does the same derivation in `_trigger_one()` and `run_dtest_variants()`
- Explicit user override of `scylla_docker_image` takes precedence (not overwritten)

## if PR is selected, update the PR description

if a user selected a PR for branch/repo selection, we can update the PR description with
the output markdown from the triggering,
we should put it under the testing section with a header like `### Testing` or similar and
then add the markdown with the list of tests that will was triggered.

this will make it easier and prevent manually copy-pasting and mistakes


## refactor function from add_nodes.py

make them use staging_trigger as a library
this libray should be easily to select from, should also have multi select
and also use code to populate specific parameters like unified packeges or scylla_repo
