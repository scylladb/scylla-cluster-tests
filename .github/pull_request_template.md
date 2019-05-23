## PR pre-checks (self review)
<!--- PR should be created as Draft, when CI finished and relevant checkboxes selected, add reviewers and then click on "Ready for review" button.-->
<!--- Put an `x` in all the boxes that apply or create PR and then click on all relevant checkboxes: -->
- [ ] I followed [KISS principle](https://en.wikipedia.org/wiki/KISS_principle)
- [ ] I gave variables/functions meaningful self-explanatory names
- [ ] I didn't leave commented-out/debugging code
- [ ] I didn't copy-paste code
- [ ] I added the relevant `backport` labels
- [ ] New configuration option are added and documented (in `sdcm/sct_config.py`)
- [ ] I have added tests to cover my changes (Infrastructure only - under `unit-test/` folder)
- [ ] All new and existing unit tests passed (`hydra unit-tests`)
- [ ] I have updated the Readme/doc folder accordingly (if needed)
