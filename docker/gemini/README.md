Currently, when releasing a new version of Gemini, there's no need to push the image to Docker Hub.
The image is built and pushed automatically by `goreleaser` when a new version is released.
Docs from Gemini repo: https://github.com/scylladb/gemini/blob/master/docs/release-process.md
Steps to release gemini :
```
0. Make sure you have proper go installed. See the version in https://github.com/scylladb/gemini/blob/master/go.mod
1. update changelog and tag the commit
2. create github token with write:packages permissions here: https://github.com/settings/tokens/new
3. export GITHUB_TOKEN="YOUR_GH_TOKEN”
4. Run `goreleaser`from`cmd/gemini`directory
```
