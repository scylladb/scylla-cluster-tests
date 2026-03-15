# Prompt Frameworks Summary

> 57 prompt engineering frameworks organized by category with components and use cases.
> Condensed from [chujianyun/skills](https://github.com/chujianyun/skills/tree/main/skills/prompt-optimizer).

## Simple Frameworks (≤3 elements)

### APE — Action, Purpose, Expectation
- **Components:** Action (task) → Purpose (why) → Expectation (desired output)
- **Use when:** Quick task instructions, code generation prompts, simple content requests
- **Example:** "Action: Write a Python email validator. Purpose: For user registration input validation. Expectation: Use regex, include error handling and unit tests."

### ERA — Expectation, Role, Action
- **Components:** Expectation (outcome) → Role (AI persona) → Action (task)
- **Use when:** Quick prompt construction, simple daily AI interactions, beginner-friendly tasks

### TAG — Task, Action, Goal
- **Components:** Task (what) → Action (how) → Goal (target outcome)
- **Use when:** Fast task definitions, simplified AI instructions, quick prototyping

### RTF — Role, Task, Format
- **Components:** Role (AI persona) → Task (what to do) → Format (output structure)
- **Use when:** Data retrieval, tutorials, tasks requiring specific response formats

### BAB — Before, After, Bridge
- **Components:** Before (current state) → After (desired state) → Bridge (how to get there)
- **Use when:** Persuasive/marketing copy, subscription promotions, product advertising

### PEE — Point, Evidence, Explanation
- **Components:** Point (claim) → Evidence (support) → Explanation (analysis)
- **Use when:** Academic writing, argumentative essays, exam answers, analytical writing

### ELI5 — Explain Like I'm 5
- **Components:** Complex topic → Simple language → Relatable analogies
- **Use when:** Simplifying complex concepts, technical documentation for non-experts, onboarding

## Medium Frameworks (4–5 elements)

### RACE — Role, Action, Context, Expectation
- **Components:** Role → Action → Context → Expectation
- **Use when:** Role-play dialogue design, customer service scripts, training simulations

### CIDI — Capture, Identify, Develop, Implement
- **Components:** Capture (problem) → Identify (root cause) → Develop (solution) → Implement (execute)
- **Use when:** Problem diagnosis, project planning, product development, change management

### SPEAR — Statement, Purpose, Evidence, Action, Result
- **Components:** Statement → Purpose → Evidence → Action → Result
- **Use when:** Persuasive writing, marketing copy, sales proposals, investor pitches

### SPAR — Situation, Problem, Action, Result
- **Components:** Situation → Problem → Action → Result
- **Use when:** Debate preparation, argumentative writing, policy analysis, critical thinking

### FOCUS — Feature, Overview, Comparison, Unique, Summary
- **Components:** Feature → Overview → Comparison → Unique aspects → Summary
- **Use when:** Product analysis, competitor research, feature evaluation, technology selection

### SMART — Specific, Measurable, Achievable, Relevant, Time-bound
- **Components:** Specific → Measurable → Achievable → Relevant → Time-bound
- **Use when:** Goal setting, project planning, OKR development, performance management

### GOPA — Goal, Obstacle, Plan, Action
- **Components:** Goal → Obstacle → Plan → Action
- **Use when:** Goal setting, action planning, project initiation, personal development

### ORID — Objective, Reflective, Interpretive, Decisional
- **Components:** Objective (facts) → Reflective (feelings) → Interpretive (meaning) → Decisional (action)
- **Use when:** Meeting facilitation, retrospectives, team discussions, experience extraction

### CARE — Context, Action, Result, Example
- **Components:** Context → Action → Result → Example
- **Use when:** Customer service, UX design, content creation guidelines, service quality

### PAUSE — Perceive, Analyze, Understand, Synthesize, Evaluate
- **Components:** Perceive → Analyze → Understand → Synthesize → Evaluate
- **Use when:** Decision-making reflection, complex problem analysis, conflict resolution

### TRACE — Task, Role, Audience, Create, Evaluate
- **Components:** Task → Role → Audience → Create → Evaluate
- **Use when:** Role-play prompt design, AI assistant configuration, interactive content

### GRADE — Goal, Request, Action, Detail, Examples
- **Components:** Goal → Request → Action → Detail → Examples
- **Use when:** Data analysis, content creation, strategy development, report generation

### TRACI — Task, Role, Audience, Create, Intent
- **Components:** Task → Role → Audience → Create → Intent
- **Use when:** Marketing communications, educational content, customer service

### RODES — Role, Objective, Details, Examples, Sense-check
- **Components:** Role → Objective → Details → Examples → Sense-check
- **Use when:** Educational content, customer service protocols, research queries

### RISE — Reflect, Inquire, Suggest, Elevate
- **Components:** Reflect → Inquire → Suggest → Elevate
- **Use when:** Giving feedback, performance reviews, mentoring, code review

### Five Ws + H — Who, What, When, Where, Why, How
- **Components:** Who → What → When → Where → Why → How
- **Use when:** News writing, problem investigation, project planning, research design

## Complex Frameworks (6+ elements)

### RACEF — Rephrase, Append, Contextualize, Examples, Follow-up
- **Components:** Rephrase (restate task) → Append (add key info) → Contextualize (set scope) → Examples (provide cases) → Follow-up (next steps)
- **Use when:** Brainstorming, data analysis, strategic planning, product development

### CRISPE — Capacity, Role, Insight, Statement, Personality, Experiment
- **Components:** Capacity → Role → Insight → Statement → Personality → Experiment
- **Use when:** Marketing campaigns, training plan design, content creation strategy

### SCAMPER — Substitute, Combine, Adapt, Modify, Put to other uses, Eliminate, Reverse
- **Components:** 7 creative thinking lenses applied to any product, service, or process
- **Use when:** Product innovation, process optimization, business model exploration

### Six Thinking Hats — White, Red, Black, Yellow, Green, Blue
- **Components:** Facts (white) → Emotions (red) → Risks (black) → Benefits (yellow) → Creativity (green) → Process (blue)
- **Use when:** Team decision meetings, project evaluation, risk assessment, strategic planning

### ROSES — Role, Objective, Scenario, Expected Solution, Steps
- **Components:** Role → Objective → Scenario → Expected Solution → Steps
- **Use when:** Role-play scene design, AI role definition, dialogue systems, game character design

### PROMPT — Persona, Requirements, Output, Model, Parameters, Tweaks
- **Components:** Persona → Requirements → Output → Model → Parameters → Tweaks
- **Use when:** Business intelligence, user personas, complex data summaries

### RISEN — Role, Instructions, Steps, End goal, Narrowing
- **Components:** Role → Instructions → Steps → End goal → Narrowing (constraints)
- **Use when:** Marketing campaigns, business plans, research papers, training modules

### RASCEF — Role, Action, Steps, Context, Examples, Format
- **Components:** Role → Action → Steps → Context → Examples → Format
- **Use when:** Technical documentation, instructional design, creative storytelling

### Atomic Prompting — Subject, Medium, Environment, Lighting, Color, Mood, Composition
- **Components:** Subject → Medium → Environment → Lighting → Color → Mood → Composition
- **Use when:** AI image generation (Midjourney, DALL-E, Adobe Firefly)

## Reasoning Frameworks

### Chain of Thought
- **Components:** Introduction (problem) → Breakdown (decompose) → Logical Progression (step-by-step) → Conclusion (synthesize)
- **Use when:** Math problems, market analysis, scientific explanations, complex reasoning

### Tree of Thought
- **Components:** Root question → Branch exploration → Evaluation → Pruning → Synthesis
- **Use when:** Strategic planning, multi-step problem solving, scenario analysis, risk assessment

## Specialized Frameworks

### Few-shot Prompting
- **Components:** 2–5 input/output examples → New input
- **Use when:** Format-specific outputs, translation, classification, code generation

### Zero-shot Prompting
- **Components:** Direct instruction with no examples
- **Use when:** Quick prototyping, general knowledge queries, simple tasks

### Chain of Destiny
- **Components:** Draft → Refine → Polish → Final (iterative improvement loops)
- **Use when:** Content creation, programming, design projects, quality-critical tasks

### COAST — Context, Objective, Actions, Scenario, Task
- **Components:** Context → Objective → Actions → Scenario → Task
- **Use when:** AI dialogue systems, chatbot development, conversational AI applications

### Bloom's Taxonomy — Remember, Understand, Apply, Analyze, Evaluate, Create
- **Components:** 6 cognitive levels from basic recall to creative synthesis
- **Use when:** Educational content design, training courses, learning objectives, assessments

### Socratic Method
- **Components:** Guided questioning → Discovery → Deeper understanding
- **Use when:** Education, critical thinking development, deep discussions, self-reflection

### HMW — How Might We
- **Components:** "How might we..." question framing for problem restatement
- **Use when:** Design thinking workshops, innovation brainstorming, product design

### RICE — Reach, Impact, Confidence, Effort
- **Components:** Reach (users) → Impact (value) → Confidence (certainty) → Effort (cost)
- **Use when:** Feature prioritization, marketing planning, budget allocation decisions

### Pros and Cons
- **Components:** Advantages list → Disadvantages list → Weighted comparison
- **Use when:** Decision analysis, product evaluation, strategy selection, investment decisions

### Challenge-Solution-Benefit
- **Components:** Challenge (pain point) → Solution (approach) → Benefit (outcome)
- **Use when:** Product marketing, sales demos, proposals, case studies

### BLOG — Background, Layout, Outline, Goal
- **Components:** Background → Layout → Outline → Goal
- **Use when:** Blog writing, content marketing, social media content, educational articles

### 4S Method — Setting, Situation, Solution, Summary
- **Components:** Setting → Situation → Solution → Summary
- **Use when:** Structured writing, presentation design, report writing, proposals

### Hamburger Model — Top bun, Filling, Bottom bun
- **Components:** Introduction (topic sentence) → Body (supporting details) → Conclusion (wrap-up)
- **Use when:** Paragraph writing instruction, essay structure, business documents

### CAR-PAR-STAR — Context-Action-Result / Problem-Action-Result / Situation-Task-Action-Result
- **Components:** Behavioral interview response patterns
- **Use when:** Interview preparation, achievement presentation, resume writing

### RELIC — Research, Evaluate, Learn, Implement, Check
- **Components:** Research → Evaluate → Learn → Implement → Check
- **Use when:** Customer feedback systems, product iteration, service quality improvement

### 3Cs Model — Company, Customer, Competitor
- **Components:** Company analysis → Customer analysis → Competitor analysis
- **Use when:** Market strategy, competitive analysis, brand positioning

### SPARK — Situation, Problem, Aspiration, Result, Kismet
- **Components:** Situation → Problem → Aspiration → Result → Kismet (serendipity)
- **Use when:** Creative problem solving, innovation, fresh perspectives

### RHODES — Role, History, Objective, Details, Expectations, Scenario
- **Components:** Role → History → Objective → Details → Expectations → Scenario
- **Use when:** Creative writing with strict style/tone requirements, customized outputs

### Imagine
- **Components:** "Imagine..." scenario framing for creative visualization
- **Use when:** Creative writing, vision planning, concept design, future forecasting

### Elicitation
- **Components:** Progressive questioning to extract requirements and knowledge
- **Use when:** Requirements gathering, user research, product discovery, knowledge acquisition

### Help Me Understand
- **Components:** "Help me understand..." framing for guided learning
- **Use when:** Learning new concepts, knowledge exploration, self-directed learning

### What If
- **Components:** "What if..." hypothetical scenario exploration
- **Use when:** Scenario planning, risk assessment, innovative thinking, strategic analysis

### TQA — Topic, Question, Answer
- **Components:** Topic → Question → Answer
- **Use when:** Q&A system design, knowledge base building, FAQ development, exam design
