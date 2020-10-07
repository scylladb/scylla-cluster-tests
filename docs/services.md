# Services

## Abstract

In sct we have repeated pattern when we run code in parallel and time to time need to sync with it, get status, make it stop.
This pattern is to be called service.
List key features that distinguish service pattern:
- Code is run in parallel
- Code is doing repetitive task
- You do one of the following:
    - Check the code status
    - Make it stop by command
    - Want it to stop automatically when:
        - Test is finished
        - Node is destroyed
        - Cluster is destroyed

## Goals
### Effortless lifecycle
Make services stopped/killed automatically, with no additional code
### Effortless dependency tracking
Make services to automatically know their dependencies, so that they could be stopped/killed in proper manner, to avoid unexpected errors to pop up
### Logging
Make logging structured so that you could see which service generated an error and which node/cluster associated with it
### Provide same interface for all threads/processes across sct
Big problem now when we have tens of threads/processes managed in different manner. It makes hard to do things like:
- Track them
- Stop or kill them, especially to do it gracefully

## Type of services

* Types by attachment
    * Detached Service
       > Service that is not attached to node or cluster
Life cycle of this type of service ends when test is finished
It will be stopped/killed only after all it's dependants are stopped/killed

    * Node Service
        > Service that is attached to a node.
Life cycle of this type of service ends when node is destroyed or test is finished
It will be stopped/killed only after all it's dependants are stopped/killed

    * Cluster Service
        > Service that is attached to a cluster.
Life cycle of this type of service ends when cluster is destroyed or test is finished
It will be stopped/killed only after all it's dependants are stopped/killed

* Types by container
    * Thread
        > Service is ran within threading.Thread
    * Process
        > Service is ran within multiprocessing.Process

## Interface

Despite types any service has following methods/interfaces:

* start()
* stop()
* kill()
* is_service_alive()
* cleanup()
* is_alive()
* wait_till_stopped(timeout)
* dependants
* tags
* service_name

For more details take a look at sdcm/services/base.py:BaseService

## List of services

This is auxiliary class that provides interface for bulk actions on list of services in chain manner.

Example:
```
# Find all services that are currently alive and have no alive dependants:
find_services_from_all_sources().alive().find_no_alive_dependants()

# Find all core services that are alive and kill them:
find_services_from_all_sources().find_by_tags('core').alive().kill()
```

For more details take a look at sdcm/services/base.py:ListOfServices
