# Jenkins Job Definitions

Due to a growing need of having extra metadata available to jenkins consumers
(both users and automation) a job define system was added to SCT jenkins generator.

It replaces previous `_display_name` file (which used to contain just the display
 name of the folder) with a special .yaml file called `_folder_definitions.yaml`.

Example definitions file:

```yaml
# used by argus to determine argus plugin
job-type: scylla-cluster-tests
# used by argus to determine which parameter wizard to use on execution
job-sub-type: longevity
# replacement for _display_name file, sets the jenkins folder display name
folder-name: Cluster - Longevity Tests
folder-description: Contains all longevity tests
# Per job (regex supported) job overrides, defines as a mapping
overrides:
  100gb: # regex, search for anything matching 100gb
      job-sub-type: artifact
  longevity-5tb-1day-gce: # specific name
    # overrides sub-type for argus, needed for folders that contain
    # for example both "artifact" and "artifact-offline" tests
      job-sub-type: rolling-upgrade
```

A job description can also be provided as an annotation, like so:

```js
/** jobDescription
    This is a simple job description.
    Can be multi line and indented.
    Will return the description without indent.
*/
```

Once template is generated the defines are applied to the job description
along with job description from the pipeline file, like so:

```
This is a simple job description.
Can be multi line and indented.
Will return the description without indent.

### JobDefinitions
job-sub-type: artifact
job-type: scylla-cluster-tests
```

If a define file was not found, a previously used mechanism is used for descriptions
```
jenkins-pipelines/oss/longevity/longevity-cdc-100gb-4h.jenkinsfile
```
