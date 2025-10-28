# Prompts Documentation

examples of useful prompts for various tasks within SCT
we should keep updating with more finegrained information to improve results

### prompt to add job descriptions to jenkinsfiles

For all `.jenkinsfile` files in the folder add a job description at the top of the file in the following format:

```groovy
/** jobDescription

    4-hour longevity test on a 6-node cluster with ~100GB dataset using mixed read/write workload.
    Runs cassandra-stress via cql-stress wrapper with SizeTieredCompactionStrategy.
    Tests SisyphusMonkey nemesis with encryption enabled (server+client) and parallel node operations.

    This test server as part of the sanity suite for longevity tests.

    Main load stress tool: cql-stress-cassandra-stress

    Labels: longevity, sanity, cql-stress, mixed-workload, encryption, size-tiered-compaction, sisyphus, nemesis, 100gb
*/
```

* it should include stress tool, if used by the configuration
* if nemesis other than NoOpMonkey is used , it should be mentioned
* it should have a list of labels/tags base on its configuration and name - shouldn't mention (spot-instance, )
* it should have a short description of what the job does, what it is goal
* it should mention if it's part of any suite (like sanity, tier1, upgrades, artifacts, etc)
* the description should be in the format of a code comment for the jenkinsfile language (groovy)
