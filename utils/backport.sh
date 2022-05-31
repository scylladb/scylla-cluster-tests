#!/usr/bin/env bash
set -x
set -e

commit_to_backport=$1

git fetch upstream


function backport() {
    branch=${1}
    echo "Backporting ${commit_to_backport} to ${branch}.."
    git checkout ${branch}
    git merge upstream/${branch}
    git cherry-pick -x ${commit_to_backport}
    git push upstream ${branch}
    echo "Done."
}


#backport "branch-2019.1"
#backport "branch-2020.1"
#backport "branch-2021.1"
backport "branch-2022.1"
#backport "branch-3.1"
#backport "branch-3.2"
#backport "branch-3.3"
#backport "branch-4.0"
#backport "branch-4.1"
#backport "branch-4.2"
#backport "branch-4.3"
#backport "branch-4.4"
#backport "branch-4.5"
#backport "branch-4.6"
backport "branch-5.0"
#backport "manager-2.0"
#backport "manager-2.1"
#backport "manager-2.2"
#backport "manager-2.6"
#backport "manager-3.0"
#backport "operator-1.1"
#backport "operator-1.2"
#backport "operator-1.3"
#backport "operator-1.4"
#backport "operator-1.5"
#backport "operator-1.6"
#backport "branch-perf-v8"
#backport "branch-perf-v9"
#backport "branch-perf-v10"
#backport "branch-perf-v11"
#backport "branch-perf-v12"
#backport "branch-perf-v13"


git checkout master
