# Kafka support in SCT

## Requirements

1) Be able to run a kafka setup, with a db cluster SCT created
2) Have any of scylla connectors installed on it
   1) from a tarball (local file / URL)
   2) from confluent-hub (latest/specific version)
3) Work locally with docker backend

## Design

![Design](./kafka-setup.png?raw=true )

### Local setup

we should be working with a https://github.com/conduktor/kafka-stack-docker-compose
and it would run where the test is running (on the SCT runner)
for case it won't be enough, we should support running it on it's own machine

### High load setup

for high load setups we can go in few routes
* **SaaS** - confluent cloud / AWS MSK / others
  - Pros:
    * All configuration and setup, and would match better how user would be using scylla connectors
    * AMS MSK, we are already working mostly in AWS, and familiar with operating its services
  - Cons:
    * Might be pricey
    * Connectivity to our test setup might be a bit more complex
    * on MSK we'll need to run your own kafka-connect instances
    * there are limitations to install custom connectors on SaaS

* **VMs** - create and configured our own VMs and cluster
  - Pros:
    * We will have full control how it would be setup
    * it's fit the current model of SCT is built (or supposed to be)
  - Cons:
    * We will need to know all the different ways to configure and optimize kafka deployment
    * We don't want to be kafka "Experts" to that level...
    * We will need to keep it updated all the time
* **K8S** - deploy using helm/operators onto k8s cluster
  -Pros:
    * We have all the code to support k8s setup on kind/EKS/GKE
    * Operation and setup into k8s should be relativity easy, and designed to scale
    * it would fit testing with scylla-operator
  - Cons:
    * manage setups with k8s isn't that easy, lots of "buttons" to play with

## Open questions

* how validation is going to be done ? with `kafka-console-consumer` ?
  with python code we'll need to write https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html ?
