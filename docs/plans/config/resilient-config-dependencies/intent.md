# Resilient test configuration dependencies

## Problem

A test run gets its configuration from an ordered list of YAML files. The
Jenkinsfile gives the list in its `test_config` parameter. A manual run gives
it with `--config` or `SCT_CONFIG_FILES`.

Some files need another file to work. An SLA configuration needs the
authentication settings in `configurations/auth_cassandra.yaml`. An LDAP
configuration needs them too. Only the Jenkinsfile states this need. The
configuration file does not state it, and SCT does not check it.

When the list misses a file that another file needs:

- SCT loads the configuration with no error.
- The test fails hours later, deep in the run, with an error that does not
  name the missing file. An example is `Unauthorized` when the test creates
  SLA roles.

The list can miss a file in three ways:

1. Jenkins keeps the parameter defaults of the previous build. After a change
   to `test_config` in a Jenkinsfile, the first build runs with the old list
   ([JENKINS-41929](https://issues.jenkins-ci.org/browse/JENKINS-41929)).
2. A person who runs a test by hand must know which files to add. Nothing
   tells them.
3. A refactor that moves shared settings to a separate file must update every
   Jenkinsfile that uses them. A missed Jenkinsfile fails at run time only.

Because of this, it is not safe to move shared settings to one file, which
is a good practice.

## Who it affects

- The tests that need authentication: the SLA, LDAP, alternator
  authorization, and rolling upgrade tests, and the TLS longevity tests.
- The SCT maintainers who move shared settings to a common file.
- The people who run a test by hand, locally or on minicloud.

## Evidence

Commit `c0bf5261b1` moved the authentication settings of 53 configuration
files to `configurations/auth_cassandra.yaml`, and added that file to the
`test_config` of 149 Jenkinsfiles. Its message says:

> Extracted common Cassandra authentication settings (authenticator,
> authenticator_user, authenticator_password, authorizer, and
> auth_superuser credentials) from 53 individual YAML test configuration
> files into a new shared file configurations/auth_cassandra.yaml.

After this commit, `rolling-upgrade-with-sla-no-shares` failed. The
authentication settings were gone from `generic-rolling-upgrade.yaml`. The
first Jenkins build used the cached `test_config` without
`auth_cassandra.yaml`. The test then failed with `Unauthorized` when it
created the SLA roles.

A count over the Jenkinsfiles on master (November 2026) finds files that
always come with `auth_cassandra.yaml`, in every Jenkinsfile that uses them:

| File | Jenkinsfiles that also list `auth_cassandra.yaml` |
|---|---|
| `nemesis/additional_configs/sla_config.yaml` | 35 of 35 |
| The six LDAP and MS AD LDAP configurations | 20 of 20 |
| `rolling-upgrade-with-sla.yaml`, `rolling-upgrade-with-sla-no-shares.yaml` | 3 of 3 |
| `generic-rolling-upgrade.yaml` | 11 of 11 |

The nemesis classes already declare the same needs in code, in
`additional_configs`. For example, the LDAP nemesis lists
`ldap-authorization.yaml` and `auth_cassandra.yaml`.

`configurations/alternator/enforce-authorization.yaml` copies the
authentication settings, with this comment:

```yaml
# NOTE: authenticator/authorizer settings below are just to ease the test case lint and the validation code
```

SCT checks only some of these needs at load time today. It checks that
`PasswordAuthenticator` has a user and a password. It does not check that
`sla: true` has `PasswordAuthenticator`, or that LDAP authorization has an
authorizer.

## Definition of done

- A configuration file can state the files that it needs. SCT loads them
  with it, for a Jenkins run, a manual run, and `lint-pipelines`.
- The files that always need authentication state that need. A run of
  `rolling-upgrade-with-sla-no-shares` without `auth_cassandra.yaml` in its
  list gets the authentication settings.
- A configuration that turns on SLA, LDAP, or alternator authorization
  without the settings that the feature needs fails at load time. The error
  names the missing setting and the file that gives it.
- `lint-pipelines` passes on every pipeline, and each test keeps its merged
  configuration. The stress command templates in `stress_template_context`
  keep their `{{ }}` expressions.
- The feature files that a Jenkinsfile adds by choice stay in the
  Jenkinsfile. Examples are `tablets_disabled.yaml`, the distro overlays,
  and the instance type overlays.

## Out of scope

- The Jenkins parameter cache for parameters other than the configuration
  list.
- Removing the now redundant `auth_cassandra.yaml` entries from the
  Jenkinsfiles and the nemesis `additional_configs`.
- A change to the merge rules of the configuration files.
- Detection of the feature in use from the stress commands, and an automatic
  load of a file that the configuration does not name.
