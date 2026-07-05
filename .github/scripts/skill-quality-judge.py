#!/usr/bin/env python3
"""Score Agent Skills quality using Claude as an LLM judge.

Evaluates SKILL.md files against a rubric covering description quality
and content quality. Outputs scores and actionable feedback.

Requires: ANTHROPIC_API_KEY environment variable.
Usage: python skill-quality-judge.py skills/skill-a skills/skill-b ...
"""

import json
import os
import sys
from pathlib import Path

try:
    import anthropic
except ImportError:
    anthropic = None

RUBRIC_PROMPT = """\
You are an expert evaluator of AI agent skills following the Agent Skills specification (agentskills.io).

Score this skill on two dimensions (0-100 each):

## Description Quality (weight: 40%)
- **Specificity**: Does it clearly state what the skill does? (not vague)
- **Trigger terms**: Does it include natural phrases a user would say that should activate this skill?
- **Completeness**: Does it cover all major capabilities?
- **Distinctiveness**: Could an agent distinguish this from other skills based on the description alone?

## Content Quality (weight: 60%)
- **Actionability**: Are instructions concrete enough for an agent to follow without guessing?
- **Conciseness**: Is it token-efficient without losing important details?
- **Workflow clarity**: Are steps logically ordered with clear decision points?
- **Progressive disclosure**: Does it keep SKILL.md focused and defer details to references/?

Respond with ONLY valid JSON (no markdown fences):
{
  "description_score": <0-100>,
  "content_score": <0-100>,
  "overall_score": <0-100>,
  "summary": "<one sentence overall assessment>",
  "suggestions": ["<actionable improvement 1>", "<actionable improvement 2>"]
}
"""


def read_skill(skill_dir: Path) -> str | None:
    skill_md = skill_dir / "SKILL.md"
    if not skill_md.exists():
        return None
    return skill_md.read_text()


def judge_skill(client, skill_name: str, content: str) -> dict:
    model = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6")
    message = client.messages.create(
        model=model,
        max_tokens=512,
        messages=[
            {"role": "user", "content": f"# Skill: {skill_name}\n\n{content}"},
        ],
        system=RUBRIC_PROMPT,
    )
    text = message.content[0].text
    return json.loads(text)


def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("::warning::ANTHROPIC_API_KEY not set. Skipping quality judge.")
        sys.exit(0)

    if anthropic is None:
        print("::error::anthropic package not installed. Run: pip install anthropic")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)

    skill_dirs = [Path(p) for p in sys.argv[1:]]
    if not skill_dirs:
        skill_dirs = sorted(Path("skills").iterdir())

    results = []
    errors = []
    failed = False
    threshold = int(os.environ.get("SKILL_QUALITY_THRESHOLD", "70"))

    for skill_dir in skill_dirs:
        if not skill_dir.is_dir():
            continue
        content = read_skill(skill_dir)
        if not content:
            continue

        skill_name = skill_dir.name
        print(f"Judging {skill_name}...")

        try:
            result = judge_skill(client, skill_name, content)
            result["skill"] = skill_name
            results.append(result)

            score = result["overall_score"]
            status = "PASS" if score >= threshold else "FAIL"
            icon = "\u2705" if score >= threshold else "\u274c"

            print(f"  {icon} {skill_name}: {score}/100 ({status})")
            print(f"     {result['summary']}")
            if result.get("suggestions"):
                for s in result["suggestions"]:
                    print(f"     - {s}")

            if score < threshold:
                failed = True
                print(f"::error file=skills/{skill_name}/SKILL.md::Score {score} below threshold {threshold}")
        except (json.JSONDecodeError, KeyError, IndexError, TypeError, anthropic.APIError) as e:
            print(f"::error file=skills/{skill_name}/SKILL.md::Failed to judge: {e}")
            errors.append({"skill": skill_name, "error": str(e)})
            failed = True
            continue

    print(f"\n{'=' * 60}")
    print(f"Results: {len(results)} skills reviewed, threshold={threshold}")
    avg = 0
    if results:
        avg = sum(r["overall_score"] for r in results) / len(results)
        print(f"Average score: {avg:.0f}/100")

    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"results={json.dumps(results)}\n")
            f.write(f"average_score={avg:.0f}\n")

    report_path = os.environ.get("SKILL_REVIEW_REPORT")
    if report_path:
        write_markdown_report(results, errors, threshold, avg, Path(report_path))

    sys.exit(1 if failed else 0)


def write_markdown_report(results: list, errors: list, threshold: int, avg: float, path: Path):
    lines = []

    if errors:
        lines.append(f":x: **{len(errors)} skill(s) failed** to evaluate\n")
        for e in errors:
            lines.append(f"- `{e['skill']}`: {e['error']}")
        lines.append("")

    if results:
        lines.append(f"**{len(results)} skills** reviewed | Average: **{avg:.0f}/100** | Threshold: {threshold}\n")
        lines.append("| Skill | Description | Content | Overall | Status |")
        lines.append("|-------|-------------|---------|---------|--------|")
        for r in sorted(results, key=lambda x: x["overall_score"]):
            score = r["overall_score"]
            icon = "\u2705" if score >= threshold else "\u274c"
            lines.append(f"| {r['skill']} | {r['description_score']} | {r['content_score']} | **{score}** | {icon} |")

        below = [r for r in results if r["overall_score"] < threshold]
        if below:
            lines.append("\n<details><summary>Suggestions for skills below threshold</summary>\n")
            for r in below:
                lines.append(f"**{r['skill']}** ({r['overall_score']}/100): {r['summary']}")
                for s in r.get("suggestions", []):
                    lines.append(f"- {s}")
                lines.append("")
            lines.append("</details>")
    elif not errors:
        lines.append("_No skills to review._")

    path.write_text("\n".join(lines))


if __name__ == "__main__":
    main()
