# Nemesis Rework (Nemesis 2.0)

## Problem Statement

Currently all nemesis are located in single file with over 7000 lines, making it hard for both humans and AIs to understand.
All logic lives in Nemesis class to facilitate sharing of functionality between nemesis, this results in extremely bloated class, making it hard to understand what we already have code for.
This results in often code duplication, where one functionality is implemented for multiple nemesis in slightly different way in different util functions.

Another problem is that Nemesis classes, which are used for filtering and carry all the flags, are actually never executed when used in SisyphusMonkey (i.e. the biggest usecase)
Methods are carriers of functionality and classes with methods are only matched through regex, i.e. never actually executed.
This method of discovery prevents further improvements

## Goals

1. **Make classes executable even with Sisyphus**
2. **Split nemesis.py file into individual modules(either files or directories) by group of nemesis**
   * Like "topology" or "modify_table"
   * Those modules would keep common functionality close but encapsulated within the same context
3. **Revisit documentation of existing nemesis**
   * Given we will be touching almost every nemesis, it makes sense to better document them all
4. **Migrate utility function from Nemesis class into their respective Nemesis classes/module**

## Proposed Solution

### Phase 1: Make Nemesis classes responsible for code (Done)

**Objective**: Make Nemesis classes be executed when run with Sisyphus with minimal changes
  * This will decouple the logic from the massive Nemesis class and serve as foundation for other phases
  * Value added here is low, but it is stepping stone for other phases

**Implementation**:
- Split Nemesis class into NemesisBaseClass and NemesisRunner
   - Abstract NemesisBaseClass is the new class for all nemesis, it contains disrupt method which is callable and executes the disruption
   - Nemesis is renamed to NemesisRunner and still contains the same method
- Change existing nemesis to have NemesisBaseClass as parent class and execute all disrupt methods on NemesisRunner
- Change the discovery and execution mechanism to work with the new classes

**Testing**:
- Unit test: Update unit tests
- Longevity: Parallel nemesis, Performance test, Short Longevity, Single nemesis run

**Expected Impact**: Almost none
  * Goal is to make the smallest amount of changes possible
  * **User facing name of the Nemesis will change from disrupt methods to classes**
---

### Phase 1a: Synchronize new nemesis names with old nemesis names in Argus

**Objective**: Allow viewing history before and after the naming change from previous phase

**Implementation**:
- Use internal map to map the old nemesis to new one in Argus


**Expected Impact**:
- History of nemesis executions will take in account both old and new names

---

### Phase 2: Split nemesis.py into directory

**Objective**: Change nemesis.py into nemesis directory
 * Group all code related to nemesis into one module
    * Easier to find relevant code
    * NemesisRegistry, NodeAllocator etc.
 * Separate Monkeys into separate submodule
   * First phase of splitting, prepared ground for Phase 3
 * Provide auto-discovery mechanism
   * NemesisRegistry currently used inheritance-based discovery, but for that to work the subclasses need to be already imported. This is not a problem with single file, but it is a problem when you have nemeses scattered across the entire module. To address this we need auto-import mechanism that will ensure that nemesis are always discovered.
 * This will be a widespread change that is better to do in separate phase

**Implementation** (To be verified with PoC):
- Convert nemesis.py into a module
- Transfer all relevant files into nemesis module
- Create auto-discovery mechanism
- Extract all Monkeys into separate submodule

**Testing**:
- Unit test: Update unit tests
- Longevity: Performance test, Short Longevity, Single nemesis run

**Expected Impact**:
- Future PRs can now split nemesis effortlessly
- Better structure of all nemesis related code
- No changes to actual tests and coverage

### Phase 2a: Update existing nemesis documentation

**Objective**: Bring existing documentation up to date

**Implementation**:
- Update existing claims about nemesis
- Add/update sections about how to run nemesis
- Add/update sections about how to develop nemesis
- Add/update section about the machinery behind nemesis
  - NemesisRegistry, NodeAllocator, NemesisJobGenerator etc.

**Expected Impact**:
- Simpler entrypoint for new developers to understand the concept and implementation

### Phase 3: Split existing nemesis into modules one by one

**Objective**: Extract all nemesis into a logically grouped modules outside of the main file

**Implementation** (Done for each group):
- Identify a group of nemesis to move
- Create module for the nemesis
  - Can be both file or module, depending on the need
- Move the classes and all of the related code
  - Improve code/util methods if needed
- Update documentation for the moved nemesis
  - Include team ownership details

**Expected Impact**:
- Codebase with clear structure with reasonably sized files
- Better nemesis code by revisiting old nemesis
