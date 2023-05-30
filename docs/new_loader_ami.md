# Introduction
This docs show how to create new loader AMI on AWS

# Steps
1. Create loader instance using SCT in e.g. us-east-1 region
1. Login to loader and install required packages:
    1. c-s (enterprise, master branch)
        ```bash
        curl http://downloads.scylladb.com/unstable/scylla-enterprise/enterprise/rpm/centos/latest/scylla.repo -o scylla.repo
        sudo mv scylla.repo /etc/yum.repos.d/scylla.repo
        sudo yum install scylla-enterprise-tools scylla-enterprise-tools-core -y
        # verify it works:
        cassandra-stress version
        cassandra-stress write cl=QUORUM n=1 -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=1) compaction(strategy=IncrementalCompactionStrategy)' -mode cql3 native -rate threads=1 -pop seq=1..1
        # it should fail with NoHostAvailableException (unless you provide valid node)
        ```
2. create image in AWS console
    1. write c-s version in description
    2. wait until process completes (status turns from Pending to Active)
3. Copy image to other regions

```
# repeat for destination_regions: ["us-west-2", "eu-west-1", "eu-west-2", "eu-north-1", "eu-central-1"]
# assuming source-region is us-east-1, otherwise adapt destination regions accordingly.
aws ec2 copy-image --region <destination_region> --name scylla-qa-loader-ami-<version> --source-region us-east-1 --source-image-id <ami_id>
```
