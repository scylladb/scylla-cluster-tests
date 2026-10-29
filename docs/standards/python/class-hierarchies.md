## Class Hierarchies

### Override Safety

SCT has deep class hierarchies, as in `BaseCluster`, `AWSCluster`, and
`MonitorSetEKS`. When you change the signature of a method in a base class:

1. Find all overrides with `grep -rn "def <method_name>" sdcm/ unit_tests/`.
2. Update each override to accept the new parameter and to forward it to
   `super()`.
3. Update the test stubs in `unit_tests/` that override the same method.
4. When an override takes `**kwargs`, make sure that the parameter gets to
   `super()`.

A missing override causes a `TypeError` at run time. The linters and the unit
tests do not find it. The methods with five or more overrides are
`add_nodes`, `_create_instances`, `wait_for_init`, and `destroy`.

### Same Change on Each Backend

A change to the code of one backend can need the same change on the other
backends. The backends are AWS, GCE, Azure, OCI, Docker, K8S, and bare metal. Check each one
and tell the result in the pull request.

Sources: `AGENTS.md` section Method Signature Changes,
`skills/code-review/SKILL.md` checks 1 and 7.
