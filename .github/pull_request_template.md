Fixes: SCT-
<!-- Finishes the issue: "Fixes: SCT-xxxx", the only form that closes it and links its backports.
     Only part of it: "Refs: SCT-xxxx", here and in the commit messages. No Jira: delete the line.
     See docs/standards/global/commits-and-pull-requests.md, "Jira Reference in the Pull Request". -->

### Testing
<!-- Add links to Argus/Jenkins of test test done with this PR -->
<!-- This would help the reviewer to cross check what was tested, and and review the results as needed -->
- [ ]

### PR pre-checks (self review)
<!--- PR should be created as Draft, when CI finished and relevant checkboxes selected, add reviewers and then click on "Ready for review" button.-->
<!--- Put an `x` in all the boxes that apply or create PR and then click on all relevant checkboxes: -->
- [ ] I added the relevant `backport` labels
- [ ] I didn't leave commented-out/debugging code

### Reminders

- Add New configuration option and document them (in `sdcm/sct_config/config.py`)
- Add unit tests to cover my changes (under `unit-test/` folder)
- Update the Readme/doc folder relevant to this change (if needed)
