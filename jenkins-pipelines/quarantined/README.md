# Quarantine of Pipelines/Config

Pipelines and test configuration might grow stale and stop working if they are not regularly used.
These files then stop serving their original purpose and only contribute to the chaos and waste time of poeple who want to use them.

Solution is to move them to a Quarantine folder, which makes it clear from the code that are not working at this moment.

Workflow for deprecating test
* Find desired test for my use case
* Simple run does not work
  * Investigate why and create issues if necessary
  * Try to do fix
    * If it works -> Submit fix
    * If it doesn't work on more fundamental level -> Move to deprecated
    * If the test doesn't make sense from product perspective -> Remove the test

Deprecation folder should not be a graveyard for pipelines/test configs and we should strive to update and fix those files.
This should happen when the pipeline is needed.

Workflow for fixing deprecated tests:
 * Find desired test for my use case
 * Check if it is not deprecated
   * If so, fix the test
 * Test
 * Submit PR with fix and removal from deprecated

Quarantine tests should not be quarantined definitely. If the test is in quarantine state for more than a year it is eligible for removal, based on consultation with owner, or if there is none, maintainers.
