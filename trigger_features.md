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

## ~~if PR is selected, update the PR description~~ (DONE)

Implemented in `utils/staging_trigger/trigger.py` and `utils/staging_trigger/cli.py`:
- `update_pr_description()` appends or replaces a `### Testing` section in the PR body
  with the markdown checklist of triggered jobs (idempotent — replaces on re-trigger)
- PR number is threaded through `prompt_for_source()` (now returns 3-tuple with PR number)
- Auto-detection of PR from current branch via `gh pr view --json number` when no PR was
  explicitly provided (works after `gh pr checkout`)
- `--update-pr / --no-update-pr` flag on the `trigger` command for non-interactive control
- Interactive prompt asks for confirmation before updating the PR description
- `run_from_config()` auto-updates the PR when `pr:` is set in the YAML config


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
generate and delete non relevant jobs for folder, maybe an option for generate command ?


## safety of conflicting parameters

for example if we set `scylla_version` to `master:latest`, and last run had some AMI set, our run would have both
and the wasn't the intention, we should pass empty string to parameters that are not set by user, to avoid confusion and unintended consequences of conflicting parameters from previous runs or defaults. This way, only the parameters that can be problematic
we should keep group of mutually exclusive parameters, and when user set one of them, we should clear the other ones in the same group, to avoid confusion and unintended consequences. This will make it more clear and easier to understand for users, and prevent mistakes.
we can warn users about conflicting parameters when they are setting them, and ask for confirmation before proceeding with the trigger. This will help prevent mistakes and ensure that users are aware of the potential consequences of their choices.

## library usage, run_multi should accumende for extra description per job

so the output markdown template would be more meaningful and useful for users, instead of just listing the job names and ids, it can also include a brief description of each job, the parameters that were used for the trigger, and any other relevant information that can help users understand what was triggered and why. This will make the output more informative and actionable for users, especially when they are triggering multiple jobs at once.


## how update from jobs would come back into the PR comment ?

resarch how we how it, maybe passing some parameter to the job, and add a final setup that would edit the comment / description ?
mayeb there are simple other way for that ?

## consider packaging this as tool of it's own ?

## how to setup credentials for jenkins
so each user can identify on it's own and not count on a shared credential,
maybe there some why to use OAuth token from gh cli, or maybe we can use some other way to authenticate with jenkins, this will make it more secure and also allow us to track who is triggering what in jenkins, instead of having all triggers coming from the same user. This will also allow us to have better control over permissions and access to jenkins, and prevent unauthorized access or misuse of the triggering tool.
