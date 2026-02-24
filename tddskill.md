GENERATE TDD SKILL FROM PACKAGE WORKFLOW

Using:
1. The open file docs/ai-skills/[package]-workflow.md
2. The open test files (observe existing test patterns)

Generate a new file: docs/ai-skills/tdd.md

Requirements:
- Extract the test framework already used (JUnit 5 / pytest / other)
- Use REAL class names from my package as examples (not generic ones)
- Use REAL method naming conventions observed in my test files
- Use REAL assertion style already in my tests

Structure it exactly like this:

---
# Skill: TDD for [package-name]

## Iron Law
[one line]

## Cycle
[RED / GREEN / REFACTOR steps, max 3 lines each]

## RED - Test Template (Java/Python)
[actual test template using my class names and conventions]

## GREEN - Rules
[max 5 bullet points]

## REFACTOR - Rules
[max 5 bullet points]

## Red Flags → Delete and Start Over
[list]

## Package-Specific Patterns
[patterns observed from my workflow.md and test files:
 - which classes are hardest to test and why
 - when to mock vs real in this package
 - common setup/teardown patterns seen]

## Next skill: docs/ai-skills/code-review.md
---

Rules for generation:
- Max 50 lines total
- No generic TypeScript/JavaScript examples
- Every example must use class names from MY package
- If test files show a pattern, replicate it exactly
