#!/usr/bin/env python3
import subprocess
import sys
from dataclasses import dataclass

start_commit = sys.argv[1]
if not start_commit:
    raise ValueError("start_commit is not set")


@dataclass
class Commit:
    hash: str
    author: str
    date: str
    title: str
    message: str

    def __str__(self):
        return self.message

    @property
    def link(self):
        return f"https://github.com/scylladb/scylla-cluster-tests/commit/{self.hash}"

    @classmethod
    def from_log(cls, log):
        lines = log.splitlines()
        hash = lines[0].split(" ", 1)[1].strip()
        author = lines[1].split(": ", 1)[1].strip()
        date = lines[4].split(": ", 1)[1].strip()
        title = lines[6].strip()
        message = "\n".join(lines[7:])
        return cls(hash, author, date, title, message)


cmd = "git log master --pretty=fuller --no-merges %s.." % (start_commit)
git_log = subprocess.check_output(cmd, shell=True).decode("utf-8")

commits = []
for commit_log_msg in git_log.split("\ncommit")[:]:
    commit = Commit.from_log(commit_log_msg)
    commits.append(commit)
commits = commits[::-1]
commit_count = len(commits)
authors_count = len(set(commit.author for commit in commits))
msg = "This short report brings to light some interesting commits to [scylla-cluster-tests.git master](https://github.com/scylladb/scylla-cluster-tests) from the last week.\n"
msg += f"Commits in the {commits[0].hash[:8]}\u2026{commits[-1].hash[:8]} range are covered.\n\n"

msg += f"There were {commit_count} non-merge commits from {authors_count} authors in that period. Some notable commits:\n\n"
msg += "\n".join(
    f"{commit.date} {commit.author} [{commit.title}]({commit.link}) {commit.message}\n" for commit in commits
)
msg += "\nSee you in the next issue of last week in scylla-cluster-tests.git master!"
print(msg)
