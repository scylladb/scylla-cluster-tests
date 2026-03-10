# Maintainers Handbook

High-level topics for the SCT maintainers handbook, organized by category.
Inspired by handbooks from the Linux kernel, Kubernetes, CPython,
CNCF projects, and GitHub's Open Source Guides.

## 1. Governance and Roles

### 1.1 Roles and Responsibilities

The project defines two roles with distinct responsibilities.

**Contributor** — anyone who submits patches, reports bugs, reviews pull requests,
or participates in discussions. Contributors are expected to:
- Verify that tests pass and new code has adequate coverage
- Check for security concerns (injection, credential leaks, OWASP top 10)
- Ensure code follows SCT conventions — see [`AGENTS.md`](../AGENTS.md) for code
  style guidelines, import rules, pytest conventions, and documentation standards
- Leave constructive feedback — explain *why*, not just *what* to change
- Run pre-commit checks before submitting — see
  [`docs/install-local-env.md`](install-local-env.md) for local setup

No special access is required; contributions and reviews go through the standard
PR process.

**Maintainer** — contributors who also have merge authority and own the long-term
health of their area. Maintainers are expected to:
- Merge PRs that meet review and CI criteria
- Triage and prioritize issues in their area
- Participate in release planning and backport decisions — see the
  [`fix-backport-conflicts`](../skills/fix-backport-conflicts/SKILL.md) skill for
  the backport workflow
- Mentor new contributors
- Keep documentation and test configurations up to date — see
  [`docs/sct-configuration.md`](sct-configuration.md) for the configuration system
- Attend regular sync meetings or communicate async status updates

Each role carries obligations, not just permissions. Having merge access without
actively reviewing and triaging is not maintainership — it is dormant access that
should be re-evaluated (see Succession Planning below).

### 1.2 Contributor Ladder

The contributor ladder defines the path from first-time contributor to maintainer.
Each level has clear entry requirements so advancement is transparent and merit-based.

| Level | Requirements | Privileges |
|-------|-------------|------------|
| **Contributor** | Submit at least 1 merged PR | Can open issues and PRs, review code, participate in discussions |
| **Maintainer** | Sustained contributions, demonstrated review quality, broad knowledge of the area, nominated and approved by existing maintainers | Merge access, release authority, CI/CD configuration access, listed in CODEOWNERS |

**Nomination process:**
1. An existing maintainer nominates the candidate with a summary of contributions
2. Other maintainers in the area review the nomination (minimum 2 approvals, no vetoes)
3. Upon approval, access is granted and the candidate is added to CODEOWNERS and team lists

<!-- TODO: document the nomination process in more detail (where it happens, template, examples) -->

**Expectations at each level are cumulative** — a maintainer is still expected to
contribute code and review PRs, not just merge.

### 1.3 Decision-Making Process

Technical decisions follow a tiered approach based on impact:

**Low impact** (bug fixes, small refactors, test additions) — a single reviewer
approval is sufficient. The PR author or a maintainer can merge.

**Medium impact** (new features, API changes, configuration additions, new test
categories) — requires review from at least two people, including one maintainer
of the affected area. Discussion happens on the PR itself. For adding new
configuration options, see [`docs/sct-configuration.md`](sct-configuration.md).

**High impact** (architectural changes, new backends, framework-wide refactors,
dependency upgrades, deprecations) — requires an implementation plan posted as a
PR for review. The plan must be approved by at least two maintainers before
implementation begins. See [`docs/plans/INSTRUCTIONS.md`](plans/INSTRUCTIONS.md)
for the plan format and the [`writing-plans`](../skills/writing-plans/SKILL.md)
skill for guidance. If consensus cannot be reached, the project lead makes the
final call.

**Principles:**
- Prefer consensus over voting — most decisions should converge through discussion
- Decisions are documented in the PR or plan that implements them, not in side channels
- "Silence is not consent" — explicitly confirm agreement for high-impact decisions
- Reversible decisions can move faster; irreversible ones (public APIs, data formats) need more scrutiny

### 1.4 OWNERS / CODEOWNERS Files

Code ownership determines who is automatically requested for reviews and who has
authority over specific areas of the codebase. The current ownership map is at
[`.github/CODEOWNERS`](../.github/CODEOWNERS).

**How ownership is assigned:**
- CODEOWNERS entries map file patterns to GitHub teams or individuals
- Ownership follows expertise — the people who wrote and maintain the code own it
- Shared ownership (2-3 people per area) prevents single points of failure

**What ownership means:**
- Owners are automatically added as reviewers on PRs touching their files
- At least one owner must approve before the PR can be merged
- Owners are responsible for triaging issues in their area
- Owners decide the technical direction for their area, within the project's overall architecture

**Example areas and their scope:**
- `sdcm/cluster_aws.py`, `sdcm/provision/aws/` — AWS backend
- `sdcm/nemesis.py`, `sdcm/nemesis_registry.py` — Nemesis framework
- `sdcm/sct_config.py`, `defaults/` — Configuration system
- `jenkins-pipelines/` — CI/CD pipelines — see [`docs/sct-pipelines.md`](sct-pipelines.md)
- `sdcm/cluster_k8s/` — Kubernetes backends — see [`docs/kubernetes_backend.md`](kubernetes_backend.md)

**Updating ownership:** When a contributor consistently reviews and maintains an area
but is not listed as owner, a maintainer should propose adding them. When an owner
becomes inactive (see Succession Planning), they should be removed.

### 1.5 Succession Planning

Maintainers step down, change roles, or become inactive. The project must handle
these transitions gracefully to avoid stalled reviews, abandoned areas, and bus-factor
risks.

**Detecting inactivity:**
- Prolonged absence of reviews, merges, or commits triggers a check-in
- A maintainer or project lead reaches out privately to ask about availability
- If there is no response, the maintainer is moved to emeritus status

<!-- TODO: define the specific inactivity policy (thresholds, who initiates, where it's tracked) -->

**Emeritus status:**
- Merge access and CODEOWNERS entries are removed
- The person is acknowledged in a contributors/emeritus list
- Emeritus maintainers can return to active status by resuming contributions and going through an expedited nomination

<!-- TODO: create an emeritus list or section in the repo -->

**Planned transitions:**
- A stepping-down maintainer should identify and mentor a successor before departing
- Knowledge transfer includes: undocumented context, ongoing work, known technical debt
- A transition period with overlapping access is recommended

**Bus-factor mitigation:**
- Every area should have at least 2 owners in CODEOWNERS
- Critical areas (CI/CD, configuration, core cluster code) should have 3+ owners
- Regularly review CODEOWNERS for single-owner areas and prioritize finding co-owners

### 1.6 Conflict Resolution

Disagreements are normal and healthy. The project uses a structured escalation path
to resolve them without damaging relationships.

<!-- TODO: document a Code of Conduct or link to one -->

**Level 1 — Discussion on the PR or issue.** Most disagreements resolve here through
back-and-forth discussion. Both parties should:
- Focus on the technical merits, not the person
- Provide concrete examples or data to support their position
- Acknowledge valid points from the other side
- Propose compromise solutions when possible

**Level 2 — Involve a third maintainer.** If the two parties cannot reach agreement
after several rounds of discussion, a third maintainer from the same area (or an
adjacent area) is asked to weigh in. The third maintainer reviews the arguments and
either sides with one position or proposes a synthesis.

**Level 3 — Project lead decision.** If Level 2 does not resolve the disagreement,
the project lead makes a binding decision. The decision is documented on the PR or
issue with the rationale. This is rare and should be treated as a signal that the
area needs clearer guidelines or an architecture decision record.

**Ground rules for all levels:**
- No personal attacks, passive aggression, or dismissive language
- Assume good intent — the other person is trying to improve the project
- "Disagree and commit" — once a decision is made, everyone supports it
- Process feedback is welcome after the fact ("we should handle this differently next time") but re-litigating decided issues is not

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
