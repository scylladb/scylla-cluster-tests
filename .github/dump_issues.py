#!/usr/bin/env python3

import csv
import json
import argparse
import subprocess
from pathlib import Path


def dump_issue_status_to_csv(repository: str, target_dir: Path = Path('issues')) -> None:
    q = """
    gh api graphql --paginate -f query='
    query Search($endCursor: String){
      search(query: "repo:$$repository", type: ISSUE, first: 100,  after: $endCursor) {
        issueCount
        pageInfo {
          startCursor
          hasNextPage
          endCursor
        }
        nodes {
          ... on PullRequest {
            number
            title
            state
            labels(first: 50, orderBy: {direction: DESC, field: CREATED_AT}) {
              nodes {
                name
              }
            }
          }
          ... on Issue {
            number
            title
            state
            labels(first: 50, orderBy: {direction: DESC, field: CREATED_AT}) {
              nodes {
                name
              }
            }
          }
        }
        pageInfo {
          endCursor
          hasNextPage
        }
      }
    }' | jq -s .
    """

    q = q.replace('$$repository', repository)
    ret = subprocess.run(q, shell=True, text=True, capture_output=True, check=True)
    pages = json.loads(ret.stdout)

    all_issues = sum([p['data']['search']['nodes'] for p in pages], [])

    assert all_issues, f'No issues found for repository {repository}'

    target_dir.mkdir(exist_ok=True, parents=True)

    with (target_dir / f'{repository.replace("/", "_")}.csv').open(mode='w') as csvfile:
        issue_writer = csv.writer(csvfile, delimiter=',', quotechar=' ',  quoting=csv.QUOTE_MINIMAL)
        for issue in all_issues:
            labels = '|'.join([l['name'] for l in issue['labels']['nodes']])
            issue_writer.writerow([
                issue['number'],
                issue['state'],
                labels,
                issue['title']])


def main():
    parser = argparse.ArgumentParser(description='Dump a github repository issues/PRs status to csv file')
    parser.add_argument('--repository', type=str, required=True,
                        help='Github repository full name i.e. scylladb/scylla-pkg')
    parser.add_argument('--target-dir', type=lambda p: Path(p).absolute(),
                        default='./issues', help='path to save the issues csv file')

    args = parser.parse_args()

    dump_issue_status_to_csv(args.repository, args.target_dir)


if __name__ == '__main__':
    main()
