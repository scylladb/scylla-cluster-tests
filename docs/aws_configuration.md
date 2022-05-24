## Preparing AWS Cloud environment
To run tests in different regions SCT needs pre-configured environment.
To configure a region: create VPC and all related environment elements (Subnet, Security Group, etc.) use::

```shell
# by default would prepare all regions supported bt SCT
hydra prepare-regions --cloud-provider aws [--region <region_name>]
# configure all the peering between regions
hydra configure-aws-peering [-r <region_name> -r <region_name>]
# create all jenkins builders
hydra configure-jenkins-builders [-r <region_name> -r <region_name>]
```

## Creating new sct-runner images

SCT can run locally and on remote Runner instance.
First of all we before creating a Runner instance we need to  create an image using::
```shell
hydra create-runner-image --cloud-provider <cloud_name> --region <region_name>
```

Then create a Runner instance (for testing it):
```shell
hydra create-runner-instance --cloud-provider <cloud_name> -r <region_name> -z <az> -t <test-id> -d <run_duration>
```
