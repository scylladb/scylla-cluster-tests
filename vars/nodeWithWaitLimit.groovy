#!groovy

// node(label) { body() } with a limit on the wait for an agent.
//
// Jenkins has no limit for the time a build waits in the queue. A timeout around node() also counts
// the time of the body. So a second parallel branch checks once a minute whether the first one has its
// agent. When no agent with the label is free after `waitLimitMinutes`, it ends the wait, sets the build
// result to NOT_BUILT and returns false. When the agent comes first, the checker ends. An abort or an
// error of the body goes to the caller unchanged.
//
// One case is accepted: when the agent comes in the same moment as the limit ends, the checker stops
// the body that has just started, and the build fails with "no agent with label".
//
// A sleep between the checks, not waitUntil: each waitUntil check adds flow nodes to the build, about
// 5800 for a 12-hour wait, against 720 sleeps.
//
// `waitLimitMinutes` null or 0: a plain node(label) { body() } with no parallel branch.
boolean call(String label, Integer waitLimitMinutes, Closure body) {
    if (!waitLimitMinutes) {
        node(label) { body() }
        return true
    }
    def acquired = false
    def limitEnded = false
    def startedMs = currentBuild.duration
    try {
        parallel(
            failFast: true,
            'run': {
                node(label) {
                    acquired = true
                    body()
                }
            },
            'wait-limit': {
                while (!acquired) {
                    if (currentBuild.duration - startedMs >= waitLimitMinutes * 60000) {
                        limitEnded = true
                        error("no agent with label ${label} in ${waitLimitMinutes} minutes")
                    }
                    sleep(time: 1, unit: 'MINUTES')
                }
            }
        )
    } catch (err) {
        if (!limitEnded || acquired) {
            throw err
        }
        currentBuild.result = 'NOT_BUILT'
        currentBuild.description = ((currentBuild.description ?: '') + "\nno lab host in ${waitLimitMinutes} min (label ${label})").trim()
        return false
    }
    return true
}
