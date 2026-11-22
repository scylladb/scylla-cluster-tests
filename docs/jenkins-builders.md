# Jenkins Builders

Builders are the Jenkins agents that SCT pipelines run on. `hydra configure-jenkins-builders`
creates them from code:

| Cloud | Code | Created in the cloud | Jenkins cloud | Label |
|---|---|---|---|---|
| AWS | `sdcm/utils/aws_builder.py` | launch template + ASG per region | EC2 Fleet | `aws-sct-builders-<region>-<VERSION>-asg` |
| GCE | `sdcm/utils/gce_builder.py` | instance template per project and region | Compute Engine | `<project>-builders-<region>-template-<VERSION>` |
| OCI | `sdcm/utils/oci_builder.py`, `builder_setup_groovy/oci_jenkind_plugin_config.groovy.tmpl` | nothing | OCI Compute | `oci-sct-builders-<region>-<VERSION>` |
| Azure | `sdcm/utils/azure_builder.py`, `builder_setup_groovy/azure_vm_agents_config.groovy.tmpl` | builder subnet, NSG and resource group `SCT-builders-<region>` | Azure VM Agents | `azure-sct-builders-<region>-<VERSION>` |

Pipelines pick a label through `vars/getJenkinsLabels.groovy`. Each branch loads `vars/` from its
own checkout, but all branches share the builders. That one fact drives the procedure below: the
label is the only switch a branch controls, so a new label is how a change reaches one branch
and not the others.

Running configure needs:
- AWS credentials for the SCT account. Configure reads the Jenkins admin credentials
  (`jenkins.json`), the GCP and OCI credentials and the builder SSH sources
  (`builder_ssh_sources.json`, see [Network access](#network-access)) from KeyStore.
- The cloud's `prepare-regions` already done (VPC, subnets, security groups).

Configure is idempotent per version: it creates what is missing and replaces the Jenkins cloud
of the same name. It never deletes an older version.

## How builders behave

- One build per builder: the plugins stop using a builder after its build (`maxTotalUses 1`;
  GCE `oneShot`) and terminate it once idle.
- Builders launch with `keep=168` and `keep_action=terminate`. If Jenkins loses track of a builder,
  `hydra-cleanup-cloud` terminates it a week after launch. OCI builders carry these as `sct.*`
  defined tags with `CreatedBy=SCT`, which is what `clean_oci` reads.
- A test longer than a week raises its builder's keep with the `builder_keep` longevity pipeline
  parameter (in hours), which `tagBuilder` writes at build start (on OCI into the `sct.keep` defined
  tag, since that's the one `clean_oci` reads). `scale-450-500` does this.
- The CI jobs (`Jenkinsfile`, `triggerMatrixPipeline`, `qa/*`) run on
  `aws-sct-builders-eu-west-1-<VERSION>-CI`, which `AwsCiBuilder` creates with `AwsBuilder`'s
  `VERSION`, so they behave the same way.

## Changing builder configuration

Any change to how builders launch is a new version, never an in-place change. This covers the
image, instance type, tags, use limits, labels, network setup and plugin settings. Re-running
configure on an existing version changes the builders of every branch at once, including release
branches that never got the change.

1. **Bump `VERSION`** of each builder class you change (`AwsBuilder`, `AwsFipsCiBuilder`,
   `GceBuilder`, `OciBuilder`, `AzureBuilder`). Names, launch templates, ASGs, instance templates and Jenkins
   clouds all include the version, so the new ones are created next to the old ones.
2. **Point `vars/getJenkinsLabels.groovy` at the new labels** in the same PR. An `AwsBuilder`
   bump also moves the CI label, used by `Jenkinsfile`, `vars/triggerMatrixPipeline.groovy` and
   `jenkins-pipelines/qa/*`, and any `<assignedNode>` in a job XML:
   `git grep -n 'sct-builders-' -- Jenkinsfile jenkins-pipelines vars ':!vars/getJenkinsLabels.groovy'`.
3. **Before merging, run configure from the PR branch** for each cloud you changed:
   ```shell
   hydra configure-jenkins-builders -c aws   # also creates the CI and FIPS ASGs
   hydra configure-jenkins-builders -c gce
   hydra configure-jenkins-builders -c oci
   hydra configure-jenkins-builders -c azure
   ```
   This only adds resources. Nothing requests the new labels yet except the PR branch itself.
   Then run `hydra check-jenkins-builder-labels`: it fails on any label of
   `vars/getJenkinsLabels.groovy` that no Jenkins node or cloud serves. OCI builders are on-demand
   clouds, so a missing OCI cloud otherwise shows up only as OCI jobs waiting forever for their label.
4. **Test from the PR branch:** run a short job on each changed cloud (for example
   `longevity-10gb-3h`, and `longevity-10gb-3h-gce` for GCE). Check that it lands on a new-version
   builder and that the builder has the expected tags.
5. **Merge.** Master moves to the new builders, and release branches stay on the old ones.
6. **Release branches opt in by backport.** A branch moves when the `getJenkinsLabels` and CI
   label changes reach it, in a full backport or on their own. In a full backport, resolve
   conflicts to master's `VERSION` and builder settings, and never run configure from a release
   branch: its builder code would replace the clouds master uses.
7. **Retire the old version** once no active branch requests its labels, including the pinned CI
   labels:
   ```shell
   git grep -n '<old label>' upstream/master upstream/branch-<x.y> -- vars Jenkinsfile jenkins-pipelines
   ```
   Then remove the old Jenkins clouds (Manage Jenkins → Clouds) and the old cloud resources:
   - AWS: the ASGs (`aws autoscaling delete-auto-scaling-group --force-delete`) and the
     `aws-sct-builders-<old VERSION>` launch template, in every region.
   - GCE: the instance templates, in every project and region.

### Example: bigger AWS builders, and a new default tag

Say AWS builders need 4 vCPUs and 16 GiB instead of 2 and 8 GiB, and every builder should carry
a new `team=qa` tag.

1. In `sdcm/utils/aws_builder.py`:
   - set `NUM_CPUS = 4` and `MemoryMiB` to 16384 in `mixed_instances_policy`;
   - add `{"Key": "team", "Value": "qa", "PropagateAtLaunch": True}` to the `Tags` list in
     `create_auto_scaling_group`;
   - bump `VERSION` from `v8` to `v9`.

   The same change elsewhere: GCE sets `machine_type` and `labels` in
   `GceBuilder._create_launch_template`; OCI sets `NUM_OCPUS`, `MEMORY_GB`, `oci_shape` and the
   `tagList` in the groovy template. On OCI a new tag key must also exist in `SCT_TAG_KEYS`
   (`sdcm/provision/oci/constants.py`), and `prepare-regions -c oci` must have created it.
2. In `vars/getJenkinsLabels.groovy`, replace `-v8-asg` with `-v9-asg`.
3. From the PR branch: `hydra configure-jenkins-builders -c aws`. This creates
   `aws-sct-builders-v9`, the `...-v9-...` ASGs and their Jenkins clouds. The v8 ASGs keep
   serving everyone.
4. From the PR branch, run `longevity-10gb-3h`. The build should land on a `v9` builder with
   4 vCPUs and the `team=qa` tag.
5. Merge. Master builds now use v9.
6. Backport the change to the release branches that should get the bigger builders.
7. When `git grep -- -v8-asg` finds nothing on any active branch, delete the v8 Jenkins clouds,
   the v8 ASGs and the `aws-sct-builders-v8` launch template.

Don't re-run configure with a changed instance type but the same `VERSION`. Configure updates the
instance requirements of an existing ASG in place, so the change would reach every branch at once.

### Why each step

- **New version, not in place:** an ASG, an instance template and a Jenkins cloud each serve every
  branch that requests their label. An in-place change hits all of them on their next build, with
  no review of what it does to a release branch's tests. GCE instance templates can't be edited
  at all.
- **Configure before merge:** the merge switches master's labels. If no cloud serves a label
  yet, every master job on it waits in the queue (see below).
- **Labels move a branch, configure runs from master:** builder code runs only at configure time,
  and every branch shares the clouds of a version. A branch needs the label change, plus any
  `vars/` change the new builders rely on; backporting the builder code too is fine, as long as
  configure runs only from master.
- **Retire last:** a release branch build may still ask for an old label months later.

### What failure looks like

- Builds stuck in the queue with "There are no nodes with the label ...": configure wasn't run
  for that version (or for that region or project), or its Jenkins cloud was removed while a
  branch still requested it.
- A builder that runs with no build on it for days: cleanup should terminate it after its keep.
  If it doesn't, check its `keep`, `keep_action` and (OCI) `CreatedBy` tags.
- `configure -c oci` fails with `NoSuchMethodException`: the OCI plugin's
  `BaremetalCloudAgentTemplate` constructor changed after a plugin upgrade. Match the
  argument list in the template to the installed version.

A new SCT runner image follows the same steps. Create the images first; see
[sct-runners.md](sct-runners.md).

## Network access

Jenkins connects to a builder over SSH, from the Jenkins controller to the public IP of
the builder. Everything else a builder does (the SCT runner, GitHub, package
repositories, Argus) is outbound. The exception is a test that runs on the builder
itself, without an SCT runner (the artifacts jobs): its nodes send their logs to the
builder from inside the cloud network.

### Who can reach a builder

From the internet: only TCP port 22, and only from the allowed sources. From inside the
cloud network: all traffic from the SCT hosts (the sources per cloud are below).

The allowed sources are kept by role in the keystore entry `builder_ssh_sources.json`,
not in this repository, because the repository is public:

```json
{
  "jenkins": ["<Jenkins controller egress>/32"],
  "<role>": ["<CIDR>", "..."]
}
```

- The `jenkins` role is mandatory. Other roles (for example an office) are optional.
- Every value is an IPv4 network. `0.0.0.0/0` is rejected, and there are no IPv6 sources.
- The role is the rule description in each cloud, so the live rules show who each
  source is.
- If the entry is missing or invalid, `hydra configure-jenkins-builders` and
  `hydra prepare-regions` stop before they change anything.

Per cloud:

- AWS: security group `SCT-builder-sg` only: TCP 22 from the allowed sources, and all
  traffic from `10.0.0.0/8`, the SCT VPCs of every region. Test nodes in another region
  reach the builder over VPC peering. Builders carry neither `SCT-ssh-sg` nor `SCT-2-sg`,
  because the shared groups also allow traffic from the internet. The group is tagged
  `keep:alive`, so the AWS cleanup keeps it while no builder runs.
- GCE: network tag `sct-builder` on the builder template, with three rules for that tag:
  `qa-vpc-builder-deny-all` (priority 60), `qa-vpc-builder-allow-ssh` and
  `qa-vpc-builder-allow-internal` (priority 50). The shared `qa-vpc` rules start at
  priority 100, so the deny rule also overrides every untagged `qa-vpc-allow-*` rule.
- OCI: builder subnet `SCT-2-subnet-regional-builder` with security list
  `SCT-2-sl-builder` (TCP 22 from the allowed sources, everything from inside the VCN).
- Azure: subnet `builders` of `SCT_IMAGES-<region>-vnet` with NSG `SCT-builder-nsg`:
  TCP 22 from the allowed sources, plus the Azure default rules (everything from inside the
  VNet, nothing else from the internet). Builders don't use `SCT_IMAGES-<region>-nsg`, which
  allows SSH from anywhere. The builder resource group is tagged `keep=alive`, so the Azure
  cleanup never deletes it; inside it, the cleanup deletes only VMs tagged `NodeType=builder`
  whose `keep` ran out, with their NIC, public IP and OS disk.

Other ports are closed to the internet too. This does not affect `hydra-show-monitor`: it
runs Grafana on a new SCT runner, not on the builder.

#### Builder versions

The builders have this network setup since AWS v6 (FIPS v6-fibs), GCE v11 and OCI v4. It
came as a new builder version, not as a change to the existing ones: older versions keep
their open network until their Jenkins cloud is removed. To roll back, move the labels in
`vars/getJenkinsLabels.groovy` (and the CI label in `Jenkinsfile`) back to the older version.

### SSH to a builder as a developer

The security standard does not allow direct SSH from laptops. Use StrongDM when the
builders are registered there, or connect from an allowed source.

To give one source access for a debugging session:

1. Add a role with the CIDR to `builder_ssh_sources.json` in both keystore backends (see
   [JSON credential](keystore-secrets-manager.md#json-credential)).
2. Run `hydra configure-jenkins-builders -c <cloud> [-r <region>]`.
3. When you are done, remove the role and run the same command again.

### Applying a change

```bash
# existing regions: the security group, rules or subnet, and the template of the current version
hydra configure-jenkins-builders -c aws -r eu-west-1
hydra configure-jenkins-builders -c aws
hydra configure-jenkins-builders -c gce
hydra configure-jenkins-builders -c oci
hydra configure-jenkins-builders -c azure

# a new region: the same pieces, as part of the region setup
hydra prepare-regions -c <cloud> -r <region>
```

- A change of the allowed sources applies at once to the security group, the firewall
  rules or the security list, so to the running builders too.
- A change of the template or the subnet needs a new builder version, see
  [Changing builder configuration](#changing-builder-configuration).

### Verification

```bash
# AWS: TCP 22 from the allowed sources, all traffic from 10.0.0.0/8
aws ec2 describe-security-groups --region <region> \
    --filters Name=tag:Name,Values=SCT-builder-sg --query 'SecurityGroups[].IpPermissions'
# AWS: every running builder of the current version has SCT-builder-sg and nothing else
aws ec2 describe-instances --region <region> \
    --filters 'Name=tag:aws:autoscaling:groupName,Values=aws-<region>-qa-builder-<version>-*' \
    Name=instance-state-name,Values=running \
    --query 'Reservations[].Instances[].[InstanceId,SecurityGroups[].GroupName]'

# GCE: the three builder rules exist and qa-vpc-ssh does not
gcloud compute firewall-rules list --project <project> --filter="name~^qa-vpc-(builder-|ssh$)"
# GCE: the builder template of the current version has the sct-builder tag
gcloud compute instance-templates list --project <project> \
    --filter="name~qa-builder-<version>$" --format="table(name,properties.tags.items)"
# GCE: the running builders of the current version have the sct-builder tag
gcloud compute instances list --project <project> \
    --filter="name~^builders AND status=RUNNING" --format="table(name,zone,tags.items)"

# OCI: no 0.0.0.0/0 or ::/0 ingress
oci network security-list list --compartment-id <compartment> --vcn-id <vcn> \
    --display-name SCT-2-sl-builder --query 'data[0]."ingress-security-rules"'

# Azure: TCP 22 from the allowed sources only
az network nsg rule list -g SCT_IMAGES-<region> --nsg-name SCT-builder-nsg -o table
```

The commands above show the configuration. Also check that it has an effect: from a
source that is not allowed (a home network or WARP), connect to a running builder and
expect a timeout. The probe covers port 22 only; the rule checks above cover the other
ports.

```bash
nc -vz -w 5 <builder public IP> 22
```
