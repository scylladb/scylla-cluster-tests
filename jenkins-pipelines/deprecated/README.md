# Deprecation of Pipelines/Config

Pipelines and test configuration might grow stale and stop working if they are not regularly used.
These files then stop serving their original purpose and only contribute to the chaos and waste time of poeple who want to use them.

Solution is to move them to a deprecation folder, which makes it clear from the code that are not working at this moment.

Workflow for deprecating test
* Find desired test for my usecase
* Simple run does not work
  * Investigate why and create issues if neccessary
  * Try to do fix
    * If it works -> Submit fix
    * If it doesn't work on more fundamental level -> Move to deprecated
    * If the test doesn't make sense from product perspective -> Remove the test

Deprecation folder should not be a graveyard for pipelines/test configs and we should strive to update and fix those files.
This should happen when the pipeline is needed.

Workflow for fixing deprecated tests:
 * Find desired test for my usecase
 * Check if it is not deprecated
   * If so, fix the test
 * Test
 * Submit PR with fix and removal from deprecated

Deprecated tests should not be deprecated definitely. If the test is in deprecated state for more than a year, it is safe to remove completely.
