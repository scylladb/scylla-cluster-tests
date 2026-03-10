# Maintainers Handbook — Topics

High-level topics for an SCT maintainers handbook, organized by category.
Topics are inspired by handbooks from the Linux kernel, Kubernetes, CPython,
CNCF projects, and GitHub's Open Source Guides.

## 1. Governance and Roles

| Topic | Description | Inspiration |
|-------|-------------|-------------|
| Maintainer roles and responsibilities | Define what maintainers, reviewers, and approvers do day-to-day | Kubernetes community-membership.md |
| Contributor ladder | Path from first-time contributor to maintainer (member, reviewer, approver, maintainer) | CNCF contributor ladder template |
| Decision-making process | How technical decisions are made, who has final say, consensus vs. authority | CPython Steering Council model |
| OWNERS / CODEOWNERS files | How code ownership is assigned and what it means for reviews | Kubernetes OWNERS mechanism |
| Succession planning | What happens when a maintainer steps down or becomes inactive | Linux kernel maintainer handbooks |
| Conflict resolution | How disagreements between maintainers or contributors are resolved | Apache Software Foundation voting model |

## 2. Code Review and Merging

| Topic | Description | Inspiration |
|-------|-------------|-------------|
| Code review expectations | What reviewers should look for (correctness, style, tests, security) | Kubernetes review guidelines |
| Review SLAs and response times | Expected turnaround for reviews to avoid stale PRs | Kubernetes oncall rotation |
| Two-phase review process | Separate technical review from approval to merge | Kubernetes LGTM + Approve model |
| Merge criteria | What must be true before a PR is merged (CI green, reviews, labels) | CPython development cycle |
| Handling large PRs | When to ask for PR splitting, how to review incrementally | Linux kernel patch series model |
| Backport process | How fixes are cherry-picked to release branches, conflict resolution | SCT fix-backport-conflicts skill |
| Commit message conventions | Format, required metadata, linking to issues | Linux kernel commit guidelines |

## 3. Release Management

| Topic | Description | Inspiration |
|-------|-------------|-------------|
| Release cadence and versioning | How often releases happen, semantic versioning rules | CPython development cycle |
| Release branch management | Branch naming, when branches are cut, who manages them | Linux kernel stable releases |
| Release checklist | Step-by-step process for cutting a release | CNCF project release templates |
| Release notes and changelogs | How to write and generate release notes | Kubernetes release notes guidelines |
| Deprecation policy | How to deprecate features, minimum notice period, migration guides | CPython deprecation policy |
| Hotfix and patch releases | When and how to issue out-of-band fixes | Linux kernel stable patch rules |

## 4. CI/CD and Test Infrastructure

| Topic | Description | Inspiration |
|-------|-------------|-------------|
| CI pipeline overview | What runs on every PR, nightly, and release | SCT Jenkins pipelines |
| Test categories and when they run | Unit, integration, performance, longevity — triggers and expectations | SCT test organization |
| Monitoring test health | How to detect flaky tests, track test pass rates | Kubernetes test grid monitoring |
| Infrastructure ownership | Who manages CI runners, cloud accounts, Docker registries | CNCF maintainer toolkit |
| Cost management | Cloud spend tracking, resource cleanup, budget alerts | CNCF project infrastructure |
| Adding new test jobs | Process for adding new Jenkins pipelines or test configurations | SCT jenkins-pipelines structure |

## 5. Issue and PR Triage

| Topic | Description | Inspiration |
|-------|-------------|-------------|
| Triage process | How incoming issues are categorized, prioritized, and assigned | Kubernetes issue triage guidelines |
| Labels and milestones | Labeling taxonomy, what labels mean, when to use milestones | Kubernetes SIG label conventions |
| Stale issue policy | When issues are marked stale, when they are closed | GitHub opensource.guide best practices |
| Bug report requirements | What information is needed to reproduce and fix a bug | CPython bug reporting guidelines |
| Feature request process | How feature requests are evaluated, accepted, or declined | opensource.guide "Learning to say no" |
| First-time contributor issues | How to identify and groom "good first issue" tasks | Kubernetes help-wanted guidelines |

## 6. Community and Communication

| Topic | Description | Inspiration |
|-------|-------------|-------------|
| Communication channels | Where discussions happen (Slack, mailing lists, GitHub Discussions) | Kubernetes community channels |
| Meeting cadence and format | Regular syncs, agendas, notes, recordings | Kubernetes SIG meetings |
| Code of conduct | Expected behavior, enforcement, reporting mechanisms | CNCF Code of Conduct |
| Onboarding new contributors | First steps, mentoring, pairing, documentation pointers | CNCF mentorship programs |
| Public roadmap | How the project roadmap is communicated and updated | CNCF project planning |
| External communications | Blog posts, conference talks, social media guidelines | CNCF marketing support for maintainers |

## 7. Security

| Topic | Description | Inspiration |
|-------|-------------|-------------|
| Security disclosure process | How vulnerabilities are reported and handled privately | CNCF security disclosure template |
| Security response team | Who is on the security team, response time expectations | Kubernetes security response committee |
| Dependency management | How dependencies are updated, vulnerability scanning | CNCF maintainer toolkit (Snyk, Docker Scout) |
| Secrets management | How credentials, API keys, and tokens are stored and rotated | SCT KeyStore patterns |
| Security review for PRs | What security-sensitive changes require extra review | OWASP top 10 awareness |

## 8. Documentation

| Topic | Description | Inspiration |
|-------|-------------|-------------|
| Documentation standards | Style guide, format, where docs live | SCT AGENTS.md documentation standards |
| Keeping docs up to date | Process for updating docs when code changes | CPython devguide maintenance |
| Configuration documentation | How config parameters are documented, auto-generation | SCT sct-configuration.md |
| Architecture decision records | How and when to document significant technical decisions | CNCF ADR practices |
| Runbooks and playbooks | Operational guides for common maintenance tasks | Kubernetes operational guides |

## 9. Maintainer Wellbeing

| Topic | Description | Inspiration |
|-------|-------------|-------------|
| Avoiding burnout | Recognizing signs, setting boundaries, taking breaks | opensource.guide maintaining balance |
| Workload distribution | Sharing oncall, rotating responsibilities, delegation | Kubernetes SIG oncall rotation |
| Saying no effectively | Declining feature requests, closing issues, setting scope | opensource.guide "Learning to say no" |
| Celebrating contributions | Recognizing contributor work, shout-outs, attribution | CNCF contributor recognition |

## 10. Development Workflow

| Topic | Description | Inspiration |
|-------|-------------|-------------|
| Git workflow | Branch strategy, rebasing vs. merging, pull request flow | Linux kernel rebasing and merging |
| Local development setup | How to set up a development environment quickly | SCT install-local-env.md |
| Pre-commit hooks and linting | What checks run locally before commits | SCT pre-commit configuration |
| Debugging and profiling | Tools and techniques for investigating issues | SCT profiling-sct-code skill |
| Feature flags and rollout | How new features are gated and gradually enabled | Kubernetes feature gates |
| Configuration management | How test and framework configuration works | SCT sct_config.py system |

## 11. Infrastructure and Operations

| Topic | Description | Inspiration |
|-------|-------------|-------------|
| Cloud backend management | AWS, GCE, Azure account setup, quotas, cost control | SCT backend documentation |
| Monitoring and alerting | Prometheus, Grafana, Argus — what is monitored and how | SCT monitoring stack |
| Log collection and analysis | How logs are gathered, stored, and searched | SCT logcollector.py |
| Incident response | What to do when CI breaks, tests fail at scale, infrastructure is down | Kubernetes postmortem process |
| Resource cleanup | Automated and manual cleanup of leaked cloud resources | SCT provision cleanup |

## Sources and References

These topics draw from the following open source maintainer guides and handbooks:

- [Linux Kernel Maintainer Handbook](https://docs.kernel.org/maintainer/index.html) — Git workflow, patch review, pull requests, subsystem maintenance
- [Kubernetes Community](https://github.com/kubernetes/community) — Governance, membership, SIG structure, contributor guide
- [Kubernetes Contributor Guide](https://www.kubernetes.dev/docs/guide/) — Onboarding, OWNERS files, review process
- [CPython Developer's Guide](https://devguide.python.org/) — Development cycle, experts index, release management
- [CNCF Maintainer Resources](https://contribute.cncf.io/maintainers/) — Templates, toolkits, governance frameworks
- [GitHub Open Source Guides](https://opensource.guide/) — Best practices, community building, maintainer wellbeing
- [GitHub Best Practices for Maintainers](https://opensource.guide/best-practices/) — Documentation, saying no, leveraging community
- [GitHub Maintaining Balance for Maintainers](https://opensource.guide/maintaining-balance-for-open-source-maintainers/) — Burnout prevention, boundary setting
