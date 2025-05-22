# Model Context Protocol for Prometheus

As of May 2025 this solution is just proof of concept (PoC).

It can either use existing Prometheus endpoint
or create local docker container and serve prometheus from there.

The MCP solution for Prometheus itself is taken from here: https://github.com/pab1it0/prometheus-mcp-server

## Setup prometheus if absent [Optional]

- Go to the `<sct-root-dir>` directory
- Setup prometheus locally using following command:
  ```<sct-root-dir>$ python3 ./mcps/setup_prometheus.py $TEST_ID```
- The above command will create local monitoring docker containers among which will be the `prometheus` one.
- Save the provided prometheus URL.

## Use already available Prometheus endpoint

- Start VSCode IDE
- Load the `<sct-root-dir>` directory in it
- Open the `.vscode/mcp.json` file
- Above the `prometheus` word in the `mcp.json` file will be button `start`, press it.
- The pop up will appear asking for a prometheus url. Put it there if default doesn't match it.
- Press enter.

## Utilizing the MCP for prometheus

If everything went well you will NOT see following message repeating periodically:

`Waiting for server to respond to `initialize` request...`

If you do see it, then MCP protocol failed to start up correctly and it is not operational.

Properly started prometheus MCP server will have following message at the end of output:

`[info] Discovered 5 tools`

If everything is ok, proceed:

- Now open the copilot chat tab.
- Select `agent` mode for LLM of your choice
- Make prompts about prometheus data

## Notes

Better to use new `copilot` chat after restart of the MCP server.

Prompts may take long making VSCode spam you with the periodic pop-up window
saying something like `The window is not responding` with options `Close`, `Keep waiting` and `Reopen`.
The proper action in this case is to wait, the restart of VSCode and/or MCP server won't help in this case.

To get info about scrapping endpoints that were alive some time ago (during test run) requires
explicit informing of the copilot about the time frame.
