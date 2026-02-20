# Background

Running on K8s with Multitenancy (i.e. more DB on one physical node) was supported but not used much recently.
With no real cases running regularly, it is not needed anymore.

Goal of this document is to document features that were required for making multitenancy work that were removed so if needed they can be revisited.

### Exclusive nemesis

Nemesis which touch VM directly in K8s environment can affect other NemesisRunners when run in parallel.
For this reason exclusive mechanism was introduced, which is a simple locking mechanism with global lock across different NemesisRunners.
NemesisRunners needs to acquire the lock before executing nemesis that is marked as "exclusive" (via class level varible in NemesisBaseClass)
