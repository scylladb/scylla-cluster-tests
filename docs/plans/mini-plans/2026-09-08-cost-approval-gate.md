# Mini-Plan: Approval Gate for Expensive Runs

**Date:** 2026-09-08
**Owner:** fruch
**Estimated LOC:** ~250
**Related Jira:** epic [SCT-851](https://scylladb.atlassian.net/browse/SCT-851), phases 3-4 (needs its own ticket)
**Depends on:** [SCT-852](https://scylladb.atlassian.net/browse/SCT-852) cost estimation — see the
`2026-09-06-sct-852-argus-cost-reporting` mini-plan

## Problem

Nothing today stops an expensive test run from starting. A misconfigured node count or
instance type is only noticed after the money is spent, and the person best placed to catch it
— the requester's team lead — never sees the number.

Once a run's cost can be estimated before provisioning (SCT-852), the missing piece is acting
on it: notify above a threshold, and for genuinely expensive runs require a second person to
agree before anything is created.

The requirement that shapes the whole design is that **someone must not be able to approve
their own run**. A gate that a requester can wave through themselves is decoration.

## Approach

**1. Gate where aborting is still free.** The check runs immediately after the run's cost is
estimated and before any resource exists — not even the SCT runner. The estimate needs the
resolved test duration, and the Argus run already exists by that point so the estimate can be
attached to it. Anywhere later and reaching the gate already costs money.

**2. Two thresholds, not one.** Below the lower threshold, nothing happens. Above it, notify
the requester and their lead so the cost is visible without blocking anyone. Above the higher
threshold, hold the run until a lead approves. Both thresholds live in configuration, so they
are reviewable and can be overridden per job rather than buried in pipeline code.

**3. Approval is restricted to a leads group.** Jenkins can restrict who may answer an
approval prompt to named users or to groups from the identity provider, and groups already
flow through to Jenkins roles here — so a leads group is the right unit rather than a
hardcoded list of usernames that silently goes stale. Such a group does not exist yet and
needs creating on the identity-provider side.

Two limits to be honest about: Jenkins administrators can always answer an approval prompt
regardless of the restriction, and answering also requires build permission on the job. The
restriction is therefore a filter, not a boundary.

**4. Self-approval is blocked by the pipeline, not by permissions.** Because administrators
bypass the restriction, the group alone cannot enforce the rule. Instead the pipeline captures
*who approved*, compares that against who requested the run, and fails the build if they are
the same person. That runs regardless of who clicked, which is what makes it an actual control.

It must **fail closed**: if the approver's identity comes back empty, the run stops rather than
proceeding on the assumption that an unknown approver is not the requester.

**5. Requester identity comes from the authenticated trigger first.** Jenkins knows who started
a run when a person started it, and that always wins. The self-declared requester parameter is
consulted only when the trigger resolves to a bot, a timer, or nothing — which is precisely the
case where it is not hand-typed, since scheduled runs take it from a code-reviewed file and
Argus-launched runs take it from Argus's own session. It may only ever *add* to the set of
people who cannot approve, never remove from it.

That parameter is now propagated to downstream jobs (fixed separately), so the data is
available where it was previously blank.

**6. Automated runs must never wait for a human.** Scheduled and bot-triggered runs have nobody
to ask and would otherwise hang forever holding an executor. Jenkins can distinguish
human-triggered builds from automated ones, and automated runs must either skip the gate or
notify without blocking. Which of the two is a policy decision that needs settling before this
is built — it is the main open question here.

**7. Notify in chat, approve in Jenkins.** The notification goes to a leads channel with a link
straight to the approval prompt; the message updates in place as the run is approved, rejected
or times out, rather than leaving a stale prompt behind. Approving happens in Jenkins so that
the approver is a real authenticated user.

Approving from inside chat with buttons is deliberately **not** the first step. Chat tooling
here has no way to hand a click back to Jenkins, so it would need a bridge — and a bridge
authenticates as itself, meaning Jenkins would record the bot as the approver. That voids both
the group restriction and the self-approval rule unless the bridge re-implements them and
becomes the trust boundary for the whole gate. Worth building later as a deliberate service,
not as a shortcut.

**8. Record every decision.** Who approved what, and when, should be captured beyond the build
log so that an administrator approving around the gate is visible after the fact. This is the
part the pipeline check cannot cover.

### Out of scope

Producing the estimate itself (SCT-852). Cost attribution, budgets and per-team quotas. Any
change to what a run costs — this only decides whether it starts.

## Files to Modify

- `vars/` -- a helper that runs the estimate, applies the thresholds, prompts for approval and
  enforces the self-approval rule; wired into the longevity pipeline as a stage between the
  duration and runner-creation stages
- `defaults/` -- the notify and approval thresholds, overridable per job
- `docs/` -- short operator note: who can approve, what happens to scheduled runs, how to
  request an exception

## Verification

- [ ] A run below the notify threshold is unaffected — no message, no prompt, no delay
- [ ] A run above the notify threshold reaches the leads channel with the estimate, and starts
      without waiting
- [ ] A run above the approval threshold does not create any resource until approved
- [ ] Approving a run started by someone else lets it proceed
- [ ] Approving your own run fails the build with a clear reason
- [ ] An approval that yields no identifiable approver fails the build rather than proceeding
- [ ] A scheduled run above the threshold behaves per the agreed policy and never waits on a human
- [ ] Rejecting or letting the prompt time out leaves nothing provisioned
- [ ] The chat message reflects the final outcome rather than remaining an open prompt
- [ ] The decision is recorded outside the build log
- [ ] `uv run sct.py pre-commit` passes
