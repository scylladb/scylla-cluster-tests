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
repositories, Argus) is outbound.

## Who can reach a builder

Only TCP port 22, and only from the allowed sources. The allowed sources are kept by
role in the keystore entry `builder_ssh_sources.json`, not in this repository, because
the repository is public:

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

- AWS: security group `SCT-builder-sg` only (TCP 22 from the allowed sources). Builders
  carry neither `SCT-ssh-sg` nor `SCT-2-sg`: they need no inbound traffic from the SCT
  network, and the shared groups allow more than SSH from the allowed sources.
- GCE: network tag `sct-builder` on the builder template, with three rules for that tag:
  `qa-vpc-builder-deny-all` (priority 200), `qa-vpc-builder-allow-ssh` and
  `qa-vpc-builder-allow-internal` (priority 100). The deny rule also overrides the
  untagged `qa-vpc-allow-*` rules.
- OCI: builder subnet `SCT-2-subnet-regional-builder` with security list
  `SCT-2-sl-builder` (TCP 22 from the allowed sources, everything from inside the VCN).

Other ports are closed too. This does not affect `hydra-show-monitor`: it runs Grafana on a
new SCT runner, not on the builder.

### Every builder version

Release branches and CI still use older builder versions, so the configuration covers all
versions in a region:

- AWS: every `aws-sct-builders*` launch template and every running builder get only `SCT-builder-sg`.
- GCE: every `gce-<project>-<region>-qa-builder*` template is replaced by a tagged copy with
  the same name. Templates whose image is gone are skipped and reported.
- OCI: only the current version moves to the builder subnet. Older versions keep the shared
  public subnet until their Jenkins cloud is removed.

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
# existing regions: the security group, rules or subnet, the template, and running builders
hydra configure-jenkins-builders -c aws -r eu-west-1
hydra configure-jenkins-builders -c aws
hydra configure-jenkins-builders -c gce
hydra configure-jenkins-builders -c oci

# a new region: the same pieces, as part of the region setup
hydra prepare-regions -c <cloud> -r <region>
```

- AWS: the launch templates of every builder version change first, then running builders
  move to `SCT-builder-sg` at once.
- GCE: the templates of every builder version get the tag first, then running builders.
- OCI: a running builder cannot change subnet. New builders start in the builder
  subnet, and the old ones stop after 6 idle minutes.

## Verification

```bash
# AWS: only TCP 22 from the allowed sources
aws ec2 describe-security-groups --region <region> \
    --filters Name=tag:Name,Values=SCT-builder-sg --query 'SecurityGroups[].IpPermissions'
# AWS: every running builder, of every version, has SCT-builder-sg and nothing else
aws ec2 describe-instances --region <region> \
    --filters 'Name=tag:aws:autoscaling:groupName,Values=aws-<region>-qa-builder-*' \
    Name=instance-state-name,Values=running \
    --query 'Reservations[].Instances[].[InstanceId,SecurityGroups[].GroupName]'

# GCE: the three builder rules exist and qa-vpc-ssh does not
gcloud compute firewall-rules list --project <project> --filter="name~^qa-vpc-(builder-|ssh$)"
# GCE: every builder template, of every version, has the sct-builder tag
gcloud compute instance-templates list --project <project> \
    --filter="name~qa-builder" --format="table(name,properties.tags.items)"
# GCE: every running builder has the sct-builder tag
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
