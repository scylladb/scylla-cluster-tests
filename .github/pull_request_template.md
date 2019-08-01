## PR pre-checks (self review)
<!--- PR should be created as Draft, when CI finished and relevant checkboxes selected, add reviewers and then click on "Ready for review" button.-->
<!--- Put an `x` in all the boxes that apply or create PR and then click on all relevant checkboxes: -->
- [ ] I followed [KISS principle](https://en.wikipedia.org/wiki/KISS_principle) and [best practices](https://docs.google.com/document/d/1jihgOKb5iGRlD8_HQ92O0JbLk1kASUoZT23i_MXFSKI)
- [ ] I gave variables/functions meaningful self-explanatory names
- [ ] I didn't leave commented-out/debugging code
- [ ] I didn't copy-paste code
- [ ] I added the relevant `backport` labels
- [ ] New configuration option are added and documented (in `sdcm/sct_config.py`)
- [ ] I have added tests to cover my changes (Infrastructure only - under `unit-test/` folder)
- [ ] All new and existing unit tests passed (CI)
- [ ] I have updated the Readme/doc folder accordingly (if needed)
