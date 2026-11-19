# Jenkins builders on Azure

**Jira:** SCT-1592

## Problem

`hydra configure-jenkins-builders` creates the Jenkins builders on AWS, GCE,
and OCI. It does not create builders on Azure. Azure jobs run on the AWS
builders in us-east-1. These builders drive an Azure test from AWS, over the
internet.

Azure is the only backend whose builders are in a different cloud. A builder
rollout reaches Azure only through AWS. The builder procedures must treat
Azure as a special case.

## Who it affects

- All Azure pipelines. They get the label `azure-eastus`, which points to the
  AWS us-east-1 builders.
- The people who roll out a runner image or a builder version. For
  CVE-2026-98432 (SCT-1423, PR #17161), we built and boot-tested the Azure
  runner image 1.21, but no Jenkins builder uses it.
- `hydra check-jenkins-builder-labels`. It cannot tell an Azure label from an
  AWS label.

## Evidence

`sct.py`, `configure_jenkins_builders`:

```python
        case "azure":
            raise NotImplementedError("configure_jenkins_builders doesn't support Azure yet")
```

`vars/getJenkinsLabels.groovy`:

```groovy
                          'azure-eastus': 'aws-sct-builders-us-east-1-v7-asg',
```

`skills/updating-runner-images/workflows/rollout.md` says that Azure has no
builders and uses the AWS us-east-1 builders.

A previous attempt to set up the Azure VM Agents plugin by hand failed. On
2026-11-19 the plugin did not load on jenkins.scylladb.com:

```
java.io.IOException: Failed to load: Azure VM Agents (azure-vm-agents 1124.v89b_2f7a_76f35)
 - Update required: Caffeine API Plugin (caffeine-api 3.2.4-208.v7e2da_a_7db_82b_) to be updated to 3.3.1-214.v18eb_56b_34920 or higher
 - Update required: commons-lang3 v3.x Jenkins API Plugin (commons-lang3-api 3.20.0-109.ve43756e2d2b_4) to be updated to 3.21.0-122.v6a_ea_4fd34049 or higher
 - Update required: Credentials Plugin (credentials 1511.v2e3cb_0008ef0) to be updated to 1514.vb_01eb_014e7d4 or higher
```

## Definition of done

- `hydra configure-jenkins-builders -c azure` creates the Azure builders, and
  a second run changes nothing.
- It works from scratch: on a region with no SCT resources and a Jenkins with
  no Azure credential, it creates every Azure resource and Jenkins object the
  builders need. Nothing is created or configured by hand.
- Jenkins starts each Azure builder on demand from the SCT runner image, as it
  does on GCE and OCI. Each builder runs one build and is then deleted.
- The builder label has a version, for example
  `azure-sct-builders-eastus-v1`. `getJenkinsLabels` points `azure-eastus` to
  it.
- From the internet, an Azure builder accepts only TCP port 22, and only from
  the sources in `builder_ssh_sources.json`.
- `hydra check-jenkins-builder-labels` passes with the Azure label.
- An Azure job runs to the end on an Azure builder.
- `docs/jenkins-builders.md` and the `updating-runner-images` skill describe
  Azure as they describe the other clouds. They have no AWS special case for
  Azure.

## Out of scope

- Azure regions other than `eastus`. `getJenkinsLabels` supports only
  `eastus` for Azure.
- A FIPS builder on Azure.
- Changes to the AWS, GCE, and OCI builders.
- The update and the restart of Jenkins that the plugin needs. The Jenkins
  admins do them.
