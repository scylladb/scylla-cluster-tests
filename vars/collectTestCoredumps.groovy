#!groovy

def call(){
    // Bound the coredump sweep to this build: on a long-lived agent the coredump directory holds
    // every dump the host ever produced (other jobs' included), and unbounded the script tars and
    // uploads all of it. Epoch seconds, consumed by upload_sct_coredump.sh via -newermt.
    // Deliberately not SCT_-prefixed: hydra.sh exports every SCT_* variable into the container and
    // sct_config rejects any that is not a documented config option.
    def sinceEpoch = (long) (currentBuild.startTimeInMillis / 1000)
    sh """#!/bin/bash

     COREDUMPS_SINCE_EPOCH=${sinceEpoch} ./utils/upload_sct_coredump.sh
    """
}
