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
PR process. AI agents (Claude via `@claude`, Copilot) can be used to assist with
code changes, reviews, and backport conflict resolution — see section 4.3 for
details. AI-generated contributions are held to the same quality and security
standards as human contributions.

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

**Nomination process (suggested, to be agreed upon by the team):**
1. An existing maintainer nominates the candidate with a summary of contributions
2. Other maintainers in the area review the nomination (minimum 2 approvals, no vetoes)
3. Upon approval, access is granted and the candidate is added to CODEOWNERS and team lists

<!-- TODO: document the nomination process in more detail (where it happens, template, examples) -->

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

**Emeritus status:**
- Merge access and CODEOWNERS entries are removed
- The person is acknowledged in a contributors/emeritus list
- Emeritus maintainers can return to active status by resuming contributions and going through an expedited nomination

<!-- TODO: create an emeritus list or section in the repo -->

**Planned transitions:**
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

### 2.1 Code Review Expectations

Every pull request must be reviewed before merging. Reviewers should check for:

- **Correctness** — does the code do what the PR description says? Are edge cases handled?
- **Test coverage** — are there unit tests for new logic? Integration tests where needed?
  See the [`writing-unit-tests`](../skills/writing-unit-tests/SKILL.md) and
  [`writing-integration-tests`](../skills/writing-integration-tests/SKILL.md) skills
  for testing guidance
- **Style and conventions** — imports at the top (no inline imports), pytest style (not
  unittest), Google docstrings. See [`AGENTS.md`](../AGENTS.md) for the full style guide
- **Security** — no credential leaks, no injection vulnerabilities, no secrets in config
  files. See section 7 (Security) for details
- **Performance** — no unnecessary loops over large datasets, no blocking calls in async
  paths, no unmocked network calls in tests

Reviewers should explain *why* something needs to change, not just request a change.
A good review comment teaches the author something for next time.

### 2.2 Review Turnaround

Stale PRs slow everyone down. To keep the review pipeline healthy:

- Prioritize reviewing others' PRs over opening new ones
- If you cannot review a PR assigned to you, reassign it or let the author know
- Authors should keep PRs small and focused to make reviews easier and faster

<!-- TODO: agree on expected review turnaround (e.g., first response within N business days) -->

### 2.3 Merge Criteria

A PR is ready to merge when all of the following are true:

- At least one approval from a reviewer (two for medium/high impact — see section 1.3)
- CI checks pass (pre-commit, unit tests, any triggered integration tests)
- No unresolved review comments
- PR description clearly explains what changed and why
- Relevant labels are applied

<!-- TODO: document the label taxonomy and which labels are required before merge -->

Pre-commit checks enforce code quality automatically — see
[`docs/contrib.md`](contrib.md) for setup instructions and
[`.pre-commit-config.yaml`](../.pre-commit-config.yaml) for the full hook list.

### 2.4 Handling Large PRs

Large PRs are harder to review and more likely to introduce bugs. When a PR is too
large:

- Ask the author to split it into smaller, self-contained PRs
- Each PR should be independently reviewable and testable
- A good split follows logical boundaries: refactoring in one PR, new feature in
  another, tests in a third if they are substantial
- If splitting is not practical (e.g., a large migration), review commit-by-commit —
  each commit should represent a coherent step

For high-impact changes, an implementation plan should define the PR breakdown
upfront. See [`docs/plans/INSTRUCTIONS.md`](plans/INSTRUCTIONS.md).

### 2.5 Backport Process

When a fix needs to be applied to a release branch:

1. The original fix is merged to the main branch first
2. A backport PR is created by cherry-picking the commit(s) to the target branch
3. If cherry-pick produces conflicts, resolve them in the backport PR

The [`fix-backport-conflicts`](../skills/fix-backport-conflicts/SKILL.md) skill
documents the full workflow for resolving backport conflicts, including how to
preserve original authorship and commit messages.

<!-- TODO: document which branches are active release branches and the backport policy (what gets backported, who decides) -->

### 2.6 Commit Message Conventions

SCT uses [Conventional Commits](https://www.conventionalcommits.org/) enforced by
commitlint. See [`commitlint.config.js`](../commitlint.config.js) for the full
configuration.

**Format:**
```
type(scope): subject

Body explaining what changed and why (minimum 30 characters).
```

**Allowed types:** `ci`, `docs`, `feature`, `fix`, `improvement`, `perf`, `refactor`,
`revert`, `style`, `test`, `unit-test`, `build`, `chore`

**Rules:**
- Scope is required and must be at least 3 characters
- Subject must be 10-120 characters, no trailing period
- Header (type + scope + subject) must be under 100 characters
- Body is required, minimum 30 characters, with a blank line after the subject
- Body lines must be under 120 characters

**Examples:**
```
fix(nemesis): handle timeout during node restart

The restart nemesis did not account for slow nodes that take longer than
the default timeout to rejoin the cluster. Extended the wait and added
a retry with exponential backoff.
```

```
feature(config): add support for custom stress tool parameters

Allow users to pass arbitrary parameters to stress tools via the
stress_cmd_custom_params configuration option. This enables testing
with non-standard workload profiles without modifying test code.
```

## 3. Supported Branches and Backports

### 3.1 Branch Naming and Purpose

SCT maintains several branch types, each tied to a ScyllaDB release line:

| Branch pattern | Purpose | Example |
|---------------|---------|---------|
| `master` | Main development branch, all new work lands here first | `master` |
| `branch-X.Y` | Tracks a ScyllaDB OSS release line | `branch-2024.2`, `branch-2025.1` |
| `branch-perf-vX` | Performance test baselines for specific release series | `branch-perf-v14` |
| `manager-X.Y` | Scylla Manager release line | `manager-3.4` |

Branch creation follows ScyllaDB release cycles — when a new ScyllaDB version
is branched, a corresponding SCT branch is created to track it.

### 3.2 Backport Labels

Every PR targeting `master` **must** carry a backport label. This is enforced by
the [`pr-require-backport-label`](../.github/workflows/pr-require-backport-label.yaml)
GitHub Action. The required label must match one of:

| Label | Meaning |
|-------|---------|
| `backport/none` | This change does not need backporting |
| `backport/X.Y` | Backport to `branch-X.Y` (e.g., `backport/2025.1`) |
| `backport/perf-vX` | Backport to `branch-perf-vX` |
| `backport/manager-X.Y` | Backport to `manager-X.Y` |

Multiple backport labels can be applied to a single PR when a fix needs to land
on several branches.

### 3.3 Automated Backport Workflow

When a PR with a `backport/X.Y` label is merged to `master`, the
[`add-label-when-promoted`](../.github/workflows/add-label-when-promoted.yaml)
workflow triggers the automated backport process
([`.github/scripts/auto-backport.py`](../.github/scripts/auto-backport.py)):

1. The script cherry-picks the PR's commits to the target branch
2. A backport PR is created automatically (authored by `scylladbbot`)
3. If cherry-pick conflicts occur, the backport PR is created as a **draft** with
   a `conflicts` label

### 3.4 Resolving Backport Conflicts

When an automated backport PR has conflicts:

1. Check out the draft PR locally
2. Resolve the conflict markers in the affected files
3. Recommit with the original author attribution preserved
4. Mark the PR as ready for review

The [`fix-backport-conflicts`](../skills/fix-backport-conflicts/SKILL.md) skill
provides the full step-by-step workflow. AI agents (Claude, Copilot) can also
resolve backport conflicts when tagged on the PR — see section 4.3.

### 3.5 What Gets Backported

- **Bug fixes** — always backport to affected branches
- **Test stability improvements** — backport when the flaky test affects the branch
- **New features** — generally do not backport unless the feature is needed for
  testing a specific release
- **Refactoring** — do not backport unless it is a prerequisite for a bug fix

<!-- TODO: formalize the backport decision criteria (who decides, escalation for disagreements) -->

## 4. CI/CD and Test Infrastructure

### 4.1 What Runs on Every PR

The main [`Jenkinsfile`](../Jenkinsfile) defines the PR validation pipeline. These
stages run automatically on every PR:

| Stage | What it does | Timeout |
|-------|-------------|---------|
| **precommit** | Runs `hydra.sh pre-commit` (linting, formatting, ruff) | 15 min |
| **unittest** | Runs `hydra.sh unit-tests` | 20 min |
| **lint test-cases** | Validates test-case YAML files | 10 min |

These stages run conditionally, triggered by **GitHub labels**:

| Stage | Trigger label(s) | What it does |
|-------|------------------|-------------|
| **integration tests** | `test-integration` | Runs `hydra.sh integration-tests` on a Docker backend |
| **provision tests** | `test-provision`, `test-provision-<backend>` | Provisions a cluster on the specified backend and runs a smoke test |
| **provision reuse** | `test-provision-<backend>-reuse` | Re-runs the provision test with `SCT_REUSE_CLUSTER` to verify cluster reuse |

Available provision backends: `aws`, `gce`, `docker`, `azure`, `k8s-local-kind-aws`,
`k8s-eks`, `xcloud-aws`, `xcloud-gce`, `vs-docker`, `vs-aws`.

### 4.2 GitHub Actions

Beyond Jenkins, several GitHub Actions workflows handle automation. The goal is to
gradually move more CI responsibilities to GitHub Actions for better integration
with the PR workflow and reduced Jenkins dependency.

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| [`pr-require-backport-label`](../.github/workflows/pr-require-backport-label.yaml) | PR open/label/sync on `master` | Enforces that every PR has a `backport/*` label |
| [`add-label-when-promoted`](../.github/workflows/add-label-when-promoted.yaml) | Push to `master` or `branch-*` | Runs auto-backport when commits are promoted |
| [`auto_assign`](../.github/workflows/auto_assign.yaml) | PR/issue opened | Auto-assigns the author to their PR/issue |
| [`claude`](../.github/workflows/claude.yml) | `@claude` mention in comments/issues | Triggers Claude AI agent for code tasks |
| [`claude-code-review`](../.github/workflows/claude-code-review.yml) | PR open/sync/ready | Automated AI code review on new PRs |
| [`call_jira_sync`](../.github/workflows/call_jira_sync.yml) | PR events | Syncs PR status to Jira tickets |
| [`build-docker-image`](../.github/workflows/build-docker-image.yaml) | `New Hydra Version` label | Builds and pushes a new Hydra Docker image |
| [`test-hydra-macos`](../.github/workflows/test-hydra-macos.yaml) | `test-macos` label | Tests Hydra on macOS runners |
| [`update-git-blame-ignore-revs`](../.github/workflows/update-git-blame-ignore-revs.yaml) | `Formatting` label | Adds formatting commits to `.git-blame-ignore-revs` |
| [`stale`](../.github/workflows/stale.yml) | Daily cron | Marks issues stale after 2 years, PRs after 1 year |
| [`cache-issues`](../.github/workflows/cache-issues.yaml) | Every 2 hours | Caches issue/PR data to S3 for cross-repo analysis |

### 4.3 AI Agents in CI

SCT integrates AI agents (Claude) into the CI workflow for code review and
task execution. See also section 2.1 for review expectations that apply to
both human and AI reviewers.

**Automated code review** — the [`claude-code-review`](../.github/workflows/claude-code-review.yml)
workflow runs on every PR from the main repository (not forks). It provides an
AI review that supplements, but does not replace, human review.

**On-demand task execution** — mentioning `@claude` in a PR comment or issue
triggers the [`claude`](../.github/workflows/claude.yml) workflow. This is
restricted to org members and collaborators. Claude can:
- Implement code changes and push commits
- Fix backport conflicts
- Answer questions about the codebase
- Run tests and analyze failures

**AI agent configuration** — behavior is governed by:
- [`CLAUDE.md`](../CLAUDE.md) — project instructions, skills, and conventions
- [`AGENTS.md`](../AGENTS.md) — codebase overview and coding standards
- [`skills/`](../skills/) — task-specific guidance (see the
  [`designing-skills`](../skills/designing-skills/SKILL.md) skill for creating new ones)

<!-- TODO: document guidelines for when to use @claude vs. doing the work manually -->

### 4.4 Label Reference

Labels serve as the primary mechanism for triggering CI stages and communicating
PR metadata.

| Label category | Labels | Purpose |
|---------------|--------|---------|
| **Backport** | `backport/none`, `backport/X.Y` | Required on every PR to `master` (see section 3.2) |
| **Provision tests** | `test-provision`, `test-provision-<backend>`, `test-provision-<backend>-reuse` | Trigger backend-specific provision tests |
| **Integration** | `test-integration` | Trigger integration test suite |
| **Docker image** | `New Hydra Version` | Build and push a new Hydra Docker image |
| **Formatting** | `Formatting` | Auto-update `.git-blame-ignore-revs` |
| **macOS** | `test-macos` | Trigger macOS Hydra test |
| **Backport status** | `conflicts` | Added to backport PRs with cherry-pick conflicts |
| **Stale** | `no-issue-activity`, `no-pr-activity` | Auto-applied by the stale bot |

<!-- TODO: document any additional labels used for issue triage or priority -->

### 4.5 Jenkins Pipeline Structure

Jenkins pipelines live in `jenkins-pipelines/` organized by test category.
See [`docs/sct-pipelines.md`](sct-pipelines.md) for the full overview.

| Directory | Purpose |
|-----------|---------|
| `jenkins-pipelines/oss/` | ScyllaDB OSS tests (longevity, upgrade, artifacts, nemesis, etc.) |
| `jenkins-pipelines/operator/` | Kubernetes operator functional tests |
| `jenkins-pipelines/performance/` | Performance regression tests |
| `jenkins-pipelines/manager/` | Scylla Manager tests |
| `jenkins-pipelines/qa/` | QA-specific test jobs |

Pipeline shared libraries are in `vars/` — these provide reusable functions for
SCT runner creation, test execution, log collection, and result reporting.

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

### 7.1 Key and Secrets Management

SCT stores credentials and keys in an S3 bucket (`scylla-qa-keystore`) accessed
via the [`KeyStore`](../sdcm/keystore.py) class. This is the current approach but
has known limitations that need improvement.

**Current state — what `KeyStore` manages:**

| Credential | S3 key | Used for |
|-----------|--------|----------|
| SSH key pairs | `scylla_test_id_ed25519`, `*.pub` | SSH access to EC2, GCE, Azure, OCI nodes |
| GCP service account | `gcp-sct-project-1.json` | GCE provisioning and API access |
| Azure credentials | `azure.json` | Azure provisioning |
| OCI credentials | `oci.json` | OCI provisioning |
| Docker Hub | `docker.json` | Pulling/pushing Docker images |
| Email config | `email_config.json` | Sending test reports |
| LDAP credentials | `ldap_ms_ad.json` | LDAP authentication tests |
| Azure/GCP KMS | `azure_kms_config.json`, `gcp_kms_config.json` | Encryption-at-rest tests |
| Argus REST | `argus_rest_credentials.json` | Test result reporting |
| Jira | `scylladb_jira.json` | Jira integration |
| Housekeeping DB | `housekeeping-db.json` | Housekeeping database access |
| Backup Azure blob | `backup_azure_blob.json` | Backup/restore tests |

**Known limitations:**
- All credentials live in a single shared S3 bucket with no per-team access control
- No automated rotation — keys are rotated manually when needed
- No audit trail for who accessed which credential and when
- SSH key is shared across all cloud backends (single `scylla_test_id_ed25519`)
- Jenkins credentials (`qa-aws-secret-key-id`, etc.) are managed separately from
  the S3 keystore with no unified view

<!-- TODO: create an implementation plan for key management improvements (per-team access, rotation, audit logging) -->

### 7.2 Security-Sensitive Changes in PRs

PRs that touch the following areas should receive extra scrutiny:

- **`sdcm/keystore.py`** — any change to credential access patterns
- **`sdcm/remote/`** — SSH and command execution (injection risks)
- **`sdcm/provision/security.py`** — security group and firewall rules
- **`.github/workflows/`** — CI workflows with `contents: write` or `id-token: write`
  permissions, especially those using `pull_request_target` (which has access to secrets)
- **`Jenkinsfile`** and `jenkins-pipelines/` — credential bindings, SCT runner access
- **Environment variables** containing `SECRET`, `KEY`, `TOKEN`, `PASSWORD`

AI-generated PRs should be reviewed with the same security standards as human PRs.
The Claude CI workflows restrict access to org members and collaborators, but
reviewers should still verify that AI-generated code does not introduce credential
leaks or injection vulnerabilities.

### 7.3 Dependency Management

Dependencies are managed via `pyproject.toml` and `uv.lock`. Security considerations:

- Review dependency updates for supply chain risks (new or changed transitive dependencies)
- Pin versions in `uv.lock` to ensure reproducible builds
- Check for known vulnerabilities before merging dependency updates

<!-- TODO: set up automated vulnerability scanning (e.g., Dependabot, Snyk, or Renovate) -->

### 7.4 GitHub Actions Security

Several workflows use elevated permissions or secrets. Key security patterns:

- **Org membership checks** — the Claude workflow verifies the commenter is an org
  member before executing. The Docker image build workflow checks team affiliation
- **`pull_request_target` caution** — workflows using this event have access to secrets
  even for fork PRs. Each such workflow includes a membership gate
- **Token scoping** — `AUTO_BACKPORT_TOKEN`, `CLAUDE_CODE_OAUTH_TOKEN`, and
  `ISSUE_ASSIGNMENT_TO_PROJECT_TOKEN` are scoped to specific operations

<!-- TODO: audit all GitHub Actions secrets and their permission scopes -->

## 8. Documentation

| Topic | Description | Reference |
|-------|-------------|-----------|
| Documentation standards | Style guide, format, where docs live | [`AGENTS.md`](../AGENTS.md) documentation standards section |
| Keeping docs up to date | Process for updating docs when code changes | <!-- TODO: document the doc-update policy --> |
| Configuration documentation | How config parameters are documented, auto-generation | [`docs/sct-configuration.md`](sct-configuration.md) |
| Architecture decision records | How and when to document significant technical decisions | [`docs/plans/`](plans/) for implementation plans |
| Runbooks and playbooks | Operational guides for common maintenance tasks | <!-- TODO: create runbooks directory --> |
| AI agent configuration | Maintaining `CLAUDE.md`, `AGENTS.md`, and skills that guide AI behavior | [`skills/designing-skills/SKILL.md`](../skills/designing-skills/SKILL.md) |

## 9. Infrastructure and Operations (provisional)

> **Note:** This section may belong in a separate operational runbook rather than
> the maintainers handbook. Keeping it here for now until we decide on the right home.

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
