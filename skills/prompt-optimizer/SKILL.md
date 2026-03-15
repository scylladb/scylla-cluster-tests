---
name: prompt-optimizer
description: >-
  Prompt engineering expert that helps craft optimized prompts using proven
  frameworks. Use when users want to optimize prompts, improve AI instructions,
  create better prompts for specific tasks, or need help selecting the best
  prompt framework for their use case.
license: CC-BY-NC-SA 4.0 (see LICENSE.txt)
origin: https://github.com/chujianyun/skills/tree/main/skills/prompt-optimizer
author: 悟鸣 (chujianyun)
---

# Prompt Optimizer

Craft high-quality, effective prompts using proven prompt engineering frameworks.

## Workflow

- [ ] Step 1: Analyze user input
- [ ] Step 2: Select framework
- [ ] Step 3: Clarify ambiguities
- [ ] Step 4: Generate optimized prompt
- [ ] Step 5: Present and iterate

### Step 1: Analyze User Input

Receive the user's request — one of:
- A raw prompt that needs optimization
- A task description or requirement
- A vague idea to turn into a prompt

### Step 2: Select Framework

Read the [references/frameworks-summary.md](references/frameworks-summary.md) to find the best match.

**Quick selection by complexity:**

| Complexity | Frameworks |
|------------|-----------|
| Simple (≤3 elements) | APE, ERA, TAG, RTF, BAB, PEE, ELI5 |
| Medium (4–5 elements) | RACE, CIDI, SPEAR, SPAR, FOCUS, SMART, GOPA, ORID, CARE, PAUSE, TRACE, GRADE, TRACI, RODES |
| Complex (6+ elements) | RACEF, CRISPE, SCAMPER, Six Thinking Hats, ROSES, PROMPT, RISEN, RASCEF, Atomic Prompting |

**Quick selection by domain:**

| Domain | Frameworks |
|--------|-----------|
| Marketing / Sales | BAB, SPEAR, Challenge-Solution-Benefit, BLOG, PROMPT, RHODES |
| Decision Analysis | RICE, Pros and Cons, Six Thinking Hats, Tree of Thought, PAUSE |
| Education / Training | Bloom's Taxonomy, ELI5, Socratic Method, PEE, Hamburger Model |
| Product Development | SCAMPER, HMW, CIDI, RELIC, 3Cs Model |
| AI Dialogue / Assistant | COAST, ROSES, TRACE, RACE, RASCEF |
| Writing / Creative | BLOG, 4S Method, Hamburger Model, Few-shot, RHODES, Chain of Destiny |
| Image Generation | Atomic Prompting |
| Quick Simple Tasks | Zero-shot, ERA, TAG, APE, RTF |
| Complex Reasoning | Chain of Thought, Tree of Thought |

**Quick selection by user intent:**

| User says | Frameworks |
|-----------|-----------|
| "I need a simple prompt" | APE, ERA, TAG |
| "I want to persuade/sell" | BAB, SPEAR, Challenge-Solution-Benefit |
| "I need to analyze/decide" | RICE, Pros and Cons, Chain of Thought |
| "I want to teach/explain" | ELI5, Bloom's Taxonomy, Socratic Method |
| "I need creative ideas" | SCAMPER, HMW, SPARK, Imagine |
| "I want structured writing" | BLOG, 4S Method, Hamburger Model |
| "I need step-by-step reasoning" | Chain of Thought, Tree of Thought |
| "I'm generating images" | Atomic Prompting |
| "I need a detailed plan" | RISEN, RASCEF, CRISPE |

### Step 3: Clarify Ambiguities

Before generating, verify with the user:
1. **Goal** — Is the intended outcome clear?
2. **Audience** — Who will receive the AI's response?
3. **Context** — Is sufficient background provided?
4. **Format** — Any specific output format needs?
5. **Constraints** — Any limitations or restrictions?

Ask clarifying questions for anything missing, ambiguous, or contradictory.

### Step 4: Generate Optimized Prompt

Apply the selected framework:
1. Structure the prompt according to framework components
2. Incorporate all clarified information
3. Ensure clarity and specificity
4. Include relevant examples if the framework requires them
5. Add necessary constraints or guidelines

### Step 5: Present and Iterate

Present the result with:
1. The selected framework name and why it was chosen
2. The complete optimized prompt
3. Brief explanation of how each framework element was applied
4. Suggestions for variations or improvements

Iterate on request while maintaining framework structure.
