#!/usr/bin/env bash

# backporting specific commits:
# ./utils/backport_sct.sh <commit_sha> <commit_sha>
# backport a PR:
# ./utils/backport_sct.sh PR#1234
#
# backport and pull, this would backport into 5.0, and after would Fast-Forward 2022.1 from 5.0
# backport "branch-5.0" ; pull "branch-5.0" "branch-2022.1"

set -e
set -o pipefail
shopt -s inherit_errexit

function reverse() {
    for (( i = ${#*}; i > 0; i-- ))
    {
        echo ${!i}
    }
}

function get_pr_commits() {
    PR_NUM=$1

    MERGE_COMMIT=$(gh pr view $PR_NUM --json mergeCommit | jq -r '.mergeCommit.oid')
    PR_COMMITS=""

    commit=$MERGE_COMMIT
    while :
    do
        COMMIT_PR=$(gh api /repos/{owner}/{repo}/commits/$commit/pulls | jq -r '.[].number')
        if [[ $COMMIT_PR == $PR_NUM ]]; then
            PR_COMMITS="$PR_COMMITS $commit"
            FOUND_FIRST=yes
        fi

        if [[ $COMMIT_PR != $PR_NUM ]] && [[ ! -z $FOUND_FIRST ]]; then
            break
        fi
        commit=$(gh api /repos/{owner}/{repo}/commits/$commit | jq -r '.parents[0].sha')
    done
    echo $(reverse $PR_COMMITS)
}


git fetch upstream

if [[ $1 == PR* ]]; then
    PR=${1//PR/}
    PR=${PR//#/}
    commit_to_backport=$(get_pr_commits ${PR})
else
    commit_to_backport=$*
fi

function pull() {
    from=${1}
    to=${2}
    ff=${3:---ff-only}
    label=${to//branch-/}

    if [[ ! -z "$PR" ]]; then
        echo "Command to label it on you own (in case of conflits):"
        echo " gh issue edit $PR --add-label \"${label}-backported\""
        echo ""
    fi
    echo "Pulling from ${from} to ${to}"
    git fetch upstream
    git branch -D ${to} || echo "no branch"
    git checkout upstream/${to} --track
    git pull upstream ${from} ${ff}
    git push upstream ${to}
    if [[ ! -z "$PR" ]]; then
        echo "labeling ${PR} with ${label}-backported"
        gh issue edit $PR --add-label "${label}-backported"
    fi
    git checkout -
    echo "Done."
}

function backport() {
    branch=${1}
    label=${branch//branch-/}
    exsiting_commits=""

    # verify if commit are backported already, and stop if they did
    for commit in $commit_to_backport
    do
        COMMIT_EXISTS=`git log upstream/${branch} | grep "$commit" || true`
        if [[ ! -z "$COMMIT_EXISTS" ]]; then
            exsiting_commits="$exsiting_commits $commit"
        fi
    done
    if [[ ! -z "$exsiting_commits" ]]; then
        echo "commits: $exsiting_commits"
        echo "Already exists in $branch"
        return
    fi

    if [[ ! -z "$PR" ]]; then
        echo "Command to label it on you own (in case of conflits):"
        echo " gh issue edit $PR --add-label \"${label}-backported\""
        echo ""
    fi

    echo "Backporting ${commit_to_backport} to ${branch}.."
    git branch -D ${branch} || echo "no branch"
    git checkout upstream/${branch} --track
    git cherry-pick -x ${commit_to_backport}
    git push upstream ${branch}
    git checkout -
    echo "Done."

    if [[ ! -z "$PR" ]]; then
        echo "labeling ${PR} with ${label}-backported"
        gh issue edit $PR --add-label "${label}-backported"
    fi
}
#backport "branch-perf-v8"
#backport "branch-perf-v10"
#backport "branch-perf-v11"
#backport "branch-perf-v12"
#backport "branch-perf-v13"

#backport "branch-2019.1"
#backport "branch-2020.1"
#backport "branch-2021.1"
#backport "branch-2022.1"

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
#backport "branch-5.0" # ; pull "branch-5.0" "branch-2022.1"

#backport "manager"
#backport "manager-2.0"
#backport "manager-2.1"
#backport "manager-2.2"
#backport "manager-2.3"
#backport "manager-2.6"
#backport "manager-3.0"

#pull "branch-4.6" "branch-4.6-upgrade-workaround" "--ff"
