# Jenkins builders: network access

The SCT Jenkins builders are the hosts that run SCT jobs. They are created from the
SCT runner image on AWS, GCE and OCI, and Jenkins starts and stops them on demand:

| Cloud | How Jenkins creates them | Lifetime |
|---|---|---|
| AWS | EC2 Fleet plugin, one auto scaling group per region | 10 idle minutes, 10 jobs at most |
| GCE | Compute Engine plugin, instance template `gce-<project>-<region>-qa-builder-<version>` | one job, then 6 minutes |
| OCI | OCI Compute plugin, builder subnet `SCT-2-subnet-regional-builder` | 6 idle minutes |

Jenkins connects to a builder over SSH, from the Jenkins controller to the public IP of
the builder. Everything else a builder does (the SCT runner, GitHub, package
repositories, Argus) is outbound. The exception is a test that runs on the builder
itself, without an SCT runner (the artifacts jobs): its nodes send their logs to the
builder from inside the cloud network.

## Who can reach a builder

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

Other ports are closed to the internet too. This does not affect `hydra-show-monitor`: it
runs Grafana on a new SCT runner, not on the builder.

### Builder versions

The builders have this network setup since AWS v6 (FIPS v6-fibs), GCE v11 and OCI v4. It
came as a new builder version, not as a change to the existing ones: older versions keep
their open network until their Jenkins cloud is removed. To roll back, move the labels in
`vars/getJenkinsLabels.groovy` (and the CI label in `Jenkinsfile`) back to the older version.

## SSH to a builder as a developer

The security standard does not allow direct SSH from laptops. Use StrongDM when the
builders are registered there, or connect from an allowed source.

To give one source access for a debugging session:

1. Add a role with the CIDR to `builder_ssh_sources.json` in both keystore backends (see
   [JSON credential](keystore-secrets-manager.md#json-credential)).
2. Run `hydra configure-jenkins-builders -c <cloud> [-r <region>]`.
3. When you are done, remove the role and run the same command again.

## Applying a change

```bash
# existing regions: the security group, rules or subnet, and the template of the current version
hydra configure-jenkins-builders -c aws -r eu-west-1
hydra configure-jenkins-builders -c aws
hydra configure-jenkins-builders -c gce
hydra configure-jenkins-builders -c oci

# a new region: the same pieces, as part of the region setup
hydra prepare-regions -c <cloud> -r <region>
```

- A change of the allowed sources applies at once to the security group, the firewall
  rules or the security list, so to the running builders too.
- A change of the template or the subnet needs a new builder version: bump `VERSION` in
  `sdcm/utils/<cloud>_builder.py` and move the Jenkins labels to it.

## Verification

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
```

The commands above show the configuration. Also check that it has an effect: from a
source that is not allowed (a home network or WARP), connect to a running builder and
expect a timeout. The probe covers port 22 only; the rule checks above cover the other
ports.

```bash
nc -vz -w 5 <builder public IP> 22
```
