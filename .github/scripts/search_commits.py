#!/usr/bin/env python3

import sys
import os
import argparse
import re
import requests

from github import Github

try:
    github_token = os.environ["GITHUB_TOKEN"]
except KeyError:
    print("Please set the 'GITHUB_TOKEN' environment variable")
    sys.exit(1)


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repository', type=str, default='scylladb/scylla-pkg', help='Github repository name')
    parser.add_argument('--commits', type=str, required=True, help='Range of promoted commits.')
    parser.add_argument('--label', type=str, default='promoted-to-master', help='Label to use')
    parser.add_argument('--ref', type=str, required=True, help='PR target branch')
    return parser.parse_args()


def main():  # noqa: PLR0914
    args = get_parser()
    github = Github(github_token)
    repo = github.get_repo(args.repository, lazy=False)
    start_commit, end_commit = args.commits.split('..')
    commits = repo.compare(start_commit, end_commit).commits

    processed_prs = set()
    for commit in commits:
        search_url = 'https://api.github.com/search/issues'
        query = f"repo:{args.repository} is:pr is:merged sha:{commit.sha}"
        params = {
            "q": query,
        }
        headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        response = requests.get(search_url, headers=headers, params=params)
        prs = response.json().get("items", [])
        for pr in prs:
            match = re.findall(r'Parent PR: #(\d+)', pr["body"])
            if match:
                pr_number = int(match[0])
                if pr_number in processed_prs:
                    continue
                ref = re.search(r'-(\d+\.\d+|perf-v(\d+))', args.ref)
                label_to_add = f'backport/{ref.group(1)}-done'
                label_to_remove = f'backport/{ref.group(1)}'
                remove_label_url = f'https://api.github.com/repos/{args.repository}/issues/{pr_number}/labels/{label_to_remove}'
                del_data = {
                    "labels": [f'{label_to_remove}']
                }
                response = requests.delete(remove_label_url, headers=headers, json=del_data)
                if response.ok:
                    print(f'Label {label_to_remove} removed successfully')
                else:
                    print(f'Label {label_to_remove} cant be removed')
            else:
                pr_number = pr["number"]
                label_to_add = args.label
            data = {
                "labels": [f'{label_to_add}']
            }
            add_label_url = f'https://api.github.com/repos/{args.repository}/issues/{pr_number}/labels'
            response = requests.post(add_label_url, headers=headers, json=data)
            if response.ok:
                print(f"Label added successfully to {add_label_url}")
            else:
                print(f"No label was added to {add_label_url}")
            processed_prs.add(pr_number)


if __name__ == "__main__":
    main()
