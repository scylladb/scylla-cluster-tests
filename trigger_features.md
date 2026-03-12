# suggestions for staging_trigger

## tab completion of job names from jenkins, and of filenames/folders

## tab completion is very slow
● Yeah, the slowness is because _JobNameType.shell_complete calls client.list_jobs_in_folder() which hits the Jenkins API on every TAB. A local cache with a short TTL (e.g. 5 minutes)
  would make it instant for repeated presses. For another time.

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


## ~~refactor function from add_nodes.py~~ (PARTIALLY DONE)

Extracted `aws_bucket_ls()` and `latest_unified_package()` into `utils/staging_trigger/package_lookup.py`.
Wired `unified_package` suggestions into the interactive parameter editor (`_prompt_for_value()`).
Created `artifacts.py` (untracked) as an example script using the library for batch artifact triggering.

Remaining: dtest helpers, `create_tree`, XML merge, perf/longevity/manager helpers.


## put description of job parameters when editing params, taken from jenkins

documentation of those should be available in the jenkins job config XML,
we can parse it and show it to the user when they are editing the parameters,
this will help them understand what each parameter does and what values are expected,
especially for parameters that have specific formats or limited choices.

we can also use the separator for better grouping and docs each group

## ~~check if questionary supports searchable in multi choice part~~ (DONE)

Implemented `use_search_filter=True` on all `questionary.checkbox()` and `questionary.select()` prompts:
- Job selection checkbox in `utils/staging_trigger/cli.py` — type to filter 100+ jobs
- Parameter editing checkbox in `utils/staging_trigger/interactive.py` — type to filter 50+ params
- Pipeline category/subcategory selects in `browse_jenkinsfiles()` — type to filter categories

Users can now type to filter with case-insensitive substring matching. Arrow keys still work for navigation.

~~in some cases we have a very long list of options or jobs, it would be nice if we could search~~

## generate trigger free style xml based on triggering options
when we trigger a job, we can generate the XML that would be used for a freestyle job with the same parameters and options,


wondering how to expose those trigger as api that would return the results from jobs ? maybe in argus ?

## generate argus views base on this tool ?


## have a rebuild option
when we trigger a job, we can have an option to rebuild the same job with the same parameters and options, this will be useful for cases where the job failed due to some transient issue and we want to quickly retry without going through the whole triggering process again.
it should stop and let you edit them if you want to change something, but it should pre-fill all the parameters with the previous values to save time.


## sync job with a folder
generate and delete non relevent jobs for folder, maybe an option for generate command ?
