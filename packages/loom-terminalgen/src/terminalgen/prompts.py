from __future__ import annotations

import json
from textwrap import dedent

from terminalgen.catalog import DomainSpec
from terminalgen.constants import DEFAULT_BASE_IMAGE
from terminalgen.models import GenerationRequest


TASK_QUALITY_REQUIREMENTS = """
        Task scope and quality
        - Treat supplied skills and seed material as candidate inspiration, not a checklist.
        - Build the task around one coherent professional objective. Complexity must come from diagnosis, edge cases, tool behavior, integration, or validation rather than unrelated subtasks.
        - Prefer one to three meaningful deliverables that all serve the same objective.
        - The initial workspace must contain realistic evidence, inputs, or broken artifacts while leaving the central work incomplete.
        - Every input artifact that the instruction says already exists must be directly present in the initial workspace. Never rely on conftest.py, hidden tests, startup hooks, or verifier-time setup to create the primary task inputs.
        - For tasks centered on parsing, transforming, analyzing, or repairing files, require a reusable script, CLI, program, or configuration as the main deliverable. It must accept or discover inputs by a documented rule; producing outputs only for the bundled example is not sufficient.
        - Reject tasks that can be solved by hard-coding outputs for the visible inputs, merely copying values into a report, or checking off unrelated mini-tasks.
        - Reject generic tutorial exercises and repeated format-conversion templates without a domain-specific reason or non-trivial behavior.
        - Do not place final outputs, answer files, precomputed expected outputs, golden files, reference outputs, or solution artifacts in the visible initial workspace. Describe small examples in the instruction when useful, but leave the actual work incomplete.
"""


TEST_QUALITY_REQUIREMENTS = """
        Test quality and fairness
        - Tests must be deterministic and executable with pytest from the /app directory. In test code, always treat /app as the workspace root.
        - Test observable behavior, outputs, exit codes, or system state. Source-text checks may only supplement behavioral tests.
        - When the task contract calls for it, edge-case or negative behavior may be checked using visible inputs or test fixtures; creating a new input is not required.
        - Compute expected results independently from visible inputs or embed only small grading constants in the hidden test file.
        - Do not require an implementation detail, library choice, exact wording, or unstated behavior unless the task instruction explicitly requires it.
        - Hidden tests may create private temporary fixtures, but they must not generate, repair, or replace the primary initial inputs that the instruction claims are already available to the task-solving agent.
"""


INSPIRATION_CRITICAL_RULES = """
        Critical Rules:
        - No Leakage: Never include code that solves the task in the prompt.
        - Verification: Prioritize tasks with clear, programmatic verification.
        - Originality: Tasks should require thought, not just copying standard tutorials.
"""


JSON_RUNTIME_PATH_REQUIREMENTS = """
        Runtime path contract
        - The task-solving agent works in /app. Start the task prompt with "Work in /app."
        - Every files[].name is relative to /app and must not start with app/ or /app/.
        - In the task prompt, refer to generated workspace files using /app/... runtime paths, never the packaging-style path app/....
        - Do not accidentally create or request /app/app/.... A nested directory named app is allowed only when it is an intentional part of the task.
        - A workspace input described as /app/foo must be supplied by files[].name = "foo". Tests must access the same path as /app/foo.
        - Audit every input path stated in the task prompt and confirm that a matching files[].name entry exists before returning the JSON object.
"""


def build_system_prompt(base_image: str = DEFAULT_BASE_IMAGE) -> str:
    return dedent(
        f"""
        You are generating high-quality tasks for AI agent training.

        Return exactly one JSON object and nothing else. Do not wrap the JSON in markdown fences.
        ### JSON Schema
        {{
          "task_id": "kebab-case-id",
          "prompt": "task description",
          "tests": "python pytest source code as a single string",
          "files": [{{"name": "relative/path", "context": "file contents"}}],
          "test_requirements": ["pytest", "..."]
        }}

        Hard requirements:
{TASK_QUALITY_REQUIREMENTS.rstrip()}
        - The task must have a clear pass/fail outcome and resemble work professionals perform in this domain.
        - Tasks should rely on actual tool installations and real system calls; the agent is permitted to install and use third-party dependencies and libraries using tools such as apt, pip, curl, npm, and wget.

        Environment construction
        - Tasks run in a pre-designed Docker environment based on: {base_image}
        - Do not include a dockerfile field in the JSON output.
        - The default workspace is rooted at /app.
        - If additional Python packages are needed for testing, list them in test_requirements.

{JSON_RUNTIME_PATH_REQUIREMENTS.rstrip()}

{TEST_QUALITY_REQUIREMENTS.rstrip()}
        
        Final validation and output
        - Before finalizing, audit prompt vs tests vs files and reject hidden requirements, contradictory defaults, impossible-to-infer outputs, path mismatches, and tasks whose tests can be passed by hard-coding visible examples.
        """
    ).strip()


SYSTEM_PROMPT = build_system_prompt()


def build_skill_system_prompt(
    domain: DomainSpec,
    *,
    base_image: str = DEFAULT_BASE_IMAGE,
) -> str:
    return _build_domain_task_system_prompt(
        intro=f"You are an expert at creating {domain.name} for AI agent training.",
        domain_description=domain.description,
        task_instruction="Create a self-contained task in this domain whose success can be determined by observable behavior.",
        domain_schema=domain.name,
        difficulty_schema="medium",
        base_image=base_image,
    )


def build_seed_system_prompt(
    domains: list[DomainSpec],
    *,
    base_image: str = DEFAULT_BASE_IMAGE,
) -> str:
    if not domains:
        raise ValueError("at least one domain is required for seed-based generation")
    domain_names = ", ".join(domain.name for domain in domains)
    domain_description = "Allowed domains:\n" + "\n".join(
        f"- {domain.name}: {domain.summary}" for domain in domains
    )
    return _build_domain_task_system_prompt(
        intro="You are an expert at creating realistic terminal tasks for AI agent training.",
        domain_description=domain_description,
        task_instruction=(
            "Infer the best domain from the seed content, then create a self-contained task "
            "whose success can be determined by observable behavior. Set info.domain to "
            f"exactly one of: {domain_names}."
        ),
        domain_schema="one-allowed-domain",
        difficulty_schema="requested-difficulty",
        base_image=base_image,
    )


def _build_domain_task_system_prompt(
    *,
    intro: str,
    domain_description: str,
    task_instruction: str,
    domain_schema: str,
    difficulty_schema: str,
    base_image: str,
) -> str:
    return dedent(
        f"""
        {intro}

        {domain_description}

        Your Task
        {task_instruction}

{TASK_QUALITY_REQUIREMENTS.rstrip()}

        Output Format:
        Return exactly one JSON object and nothing else. Do not wrap the JSON in markdown fences.
        ### JSON Schema
        {{
          "task_id": "kebab-case-id",
          "prompt": "task description with explicit requirements",
          "tests": "python pytest source code as a single string",
          "info": {{"domain": "{domain_schema}", "difficulty": "{difficulty_schema}"}},
          "files": [
            {{"name": "relative/path", "context": "file contents"}}
          ],
          "test_requirements": ["pytest", "..."]
        }}

{JSON_RUNTIME_PATH_REQUIREMENTS.rstrip()}

{TEST_QUALITY_REQUIREMENTS.rstrip()}
        - Do not include any content related to tests file in the task description, Test files are not visible within the workspace.

        Final validation and output:
        - Before finalizing, audit prompt vs tests vs files and reject hidden requirements, contradictory defaults, impossible-to-infer outputs, path mismatches, and tasks whose tests can be passed by hard-coding visible examples.
        """
    ).strip()


def build_skill_prompt(
    request: GenerationRequest,
    domain: DomainSpec,
    *,
    base_image: str = DEFAULT_BASE_IMAGE,
) -> str:
    skills_json = json.dumps(request.skills, ensure_ascii=False, indent=2)
    prompt = dedent(
        f"""
        #Task Generation Request

        ##Primitive Skills (Building Blocks)
        __SKILLS_JSON__

{INSPIRATION_CRITICAL_RULES.rstrip()}

        ##Pre-designed Docker Environment
        Tasks will run in this pre-designed Docker environment {base_image}
        If additional packages are needed for testing, list them in "test_requirements".

        ##Instructions
        CREATE A NOVEL TASK that:
        1. Treats the primitive list as candidates rather than a checklist
        2. Chooses exactly one primitive as the anchor skill
        3. Uses at most two supporting primitives, only when required by the same workflow
        4. Has one coherent professional objective with clear, unambiguous specifications
        5. Derives difficulty from reasoning, edge cases, integration, or validation rather than the number of deliverables

        Avoid decorative or overly theatrical scenarios.
        The task should feel like work someone would plausibly do in a local terminal environment.
        Do not try to cover every selected primitive. Ignore candidates that do not fit the anchor workflow naturally.
        """
    ).strip()
    return prompt.replace("__SKILLS_JSON__", skills_json)


def build_plan_system_prompt() -> str:
    return dedent(
        """
        Design a compact, realistic terminal task plan for another agent to author.
        Return exactly one JSON object matching this schema and nothing else:

        {
          "domain": "one allowed domain",
          "difficulty": "requested difficulty",
          "objective": "one sentence",
          "initial_state": "compact file/path inventory and broken state",
          "workflow": ["2-8 top-level stages"],
          "deliverables": ["1-4 paths or artifacts"],
          "verification": ["1-10 observable checks"],
          "constraints": ["0-8 exceptional constraints"]
        }

        Hard limits and compression rules:
        - Keep the combined field text under 2500 characters. Never repeat a fact across fields.
        - Use one sentence for objective and semicolon-separated path entries for initial_state.
        - Normally use 2-5 workflow stages and 3-6 checks. Use the schema maxima only when needed.
        - Deliverables name artifacts; workflow states work; verification states outcomes.
        - Omit default constraints already guaranteed by the authoring agent, including `/app` as
          runtime root, deterministic tests, no hidden-test leakage, and no hard-coded answers.
        - Each item must be a short phrase or sentence, never a paragraph or implementation recipe.

        The plan is a specification, not a solution. Exclude implementation code, pytest source, reference answers,
        solved artifacts, implementation choices, commands, and explanatory prose. Keep the initial
        workspace realistic and directly creatable. Require reusable observable behavior. Use only
        `/app/...` runtime paths; never use workspace/package-authoring paths or hidden test paths.
        Workflow items are top-level stages. Narrow the scope if more than eight are required.
        Schema limits remain: workflow 2-8, deliverables 1-4, verification 1-10, constraints 0-8.
        """
    ).strip()


def build_plan_prompt(
    request: GenerationRequest,
    domains: list[DomainSpec],
    *,
    base_image: str = DEFAULT_BASE_IMAGE,
) -> str:
    if not domains:
        raise ValueError("at least one domain is required for task planning")
    domain_payload = [
        {"name": domain.name, "summary": domain.summary}
        for domain in domains
    ]
    if request.atomic_card is not None:
        bucket_rule = _atomic_bucket_rule(request)
        source = {
            "type": "atomic-weakness-card",
            "card": request.atomic_card.model_dump(),
            "variant_bucket": request.variant_bucket.value if request.variant_bucket else None,
            "variant_index": request.variant_index,
            "template_family_id": request.template_family_id,
        }
        source_rules = [
            "Design a novel task that exercises the named capability without copying the source benchmark task, wording, assets, constants, or solution.",
            "Make every required gate observable and preserve the forbidden-shortcut exclusions.",
            "Use the requested variant bucket as the primary diversity mechanism.",
            bucket_rule,
        ]
    elif request.seed_content is not None:
        source = {
            "type": "seed",
            "seed_id": request.seed_record.seed_id if request.seed_record else "unknown",
            "content": request.seed_content,
        }
        source_rules = [
            "Infer the best domain from the seed and choose exactly one allowed domain.",
            "Preserve useful technical intent, but design a new task rather than copying a supplied solution or completed artifact.",
        ]
    else:
        source = {"type": "skills", "candidates": request.skills}
        source_rules = [
            "Use one candidate as the anchor and up to three directly related supporting skills.",
            "Ignore candidates that do not share the same inputs, state, deliverable, or validation workflow.",
        ]
    payload = {
        "generation_mode": request.generation_mode.value,
        "difficulty": request.difficulty,
        "base_image": base_image,
        "allowed_domains": domain_payload,
        "source": source,
        "requirements": source_rules,
    }
    return dedent(
        f"""
        Create one task-authoring plan from this request:

        {json.dumps(payload, ensure_ascii=False, indent=2)}

        Select the exact domain name from allowed_domains and copy the requested difficulty.
        The downstream agent receives only this plan. Make it self-contained but compact; include
        each fact once and stay within the system prompt's total text budget.
        """
    ).strip()


def build_agent_task_prompt(
    request: GenerationRequest,
    domain: DomainSpec | None,
    *,
    base_image: str = DEFAULT_BASE_IMAGE,
    domain_choices: list[DomainSpec] | None = None,
) -> str:
    skills = "\n".join(f"- {skill}" for skill in request.skills) or "- none"
    choices = domain_choices or []
    if domain is None and not choices:
        raise ValueError("domain choices are required for automatic seed domain selection")

    if domain is not None:
        manifest_domain = domain.name
        target_domain = dedent(
            f"""
            - domain: {domain.name}
            - difficulty: {request.difficulty}
            - generation_mode: {request.generation_mode.value}
            - sample_index: {request.sample_index}

            {domain.description.strip()}
            """
        ).strip()
    else:
        manifest_domain = "one-allowed-domain"
        allowed_domains = "\n".join(
            f"- {choice.name}: {choice.summary}" for choice in choices
        )
        target_domain = dedent(
            f"""
            Infer the best domain from the seed content and write its exact name to
            `task.json` at `info.domain`. Choose exactly one allowed domain:

            {allowed_domains}

            - difficulty: {request.difficulty}
            - generation_mode: {request.generation_mode.value}
            - sample_index: {request.sample_index}
            """
        ).strip()

    atomic_section = ""
    if request.atomic_card is not None:
        atomic_payload = {
            "card": request.atomic_card.model_dump(),
            "variant_bucket": request.variant_bucket.value if request.variant_bucket else None,
            "variant_index": request.variant_index,
            "template_family_id": request.template_family_id,
        }
        atomic_section = dedent(
            f"""
            ## Atomic Capability Contract

            Build a fresh task around this abstract capability contract. The source task name is
            provenance only: do not copy its wording, files, constants, domain story, tests, or
            solution. Make every required gate observable and ensure each forbidden shortcut fails.
            Apply this bucket-specific rule: {_atomic_bucket_rule(request)}

            ```json
            {json.dumps(atomic_payload, ensure_ascii=False, indent=2)}
            ```
            """
        ).strip()

    if request.plan is not None:
        compact_plan = request.plan.model_dump(exclude={"domain", "difficulty"})
        if not compact_plan["constraints"]:
            compact_plan.pop("constraints")
        plan_payload = json.dumps(
            compact_plan,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        inspiration_section = dedent(
            f"""
            ## Compact Task Plan
            {plan_payload}

            {atomic_section}
            """
        ).strip()
        inspiration_rule = (
            "Follow the compact plan as the complete design contract without expanding its scope."
        )
    elif request.atomic_card is not None:
        inspiration_section = atomic_section
        inspiration_rule = (
            "Treat the atomic capability contract as mandatory. Produce a novel instance in the "
            "requested variant bucket and expose every required gate to the verifier."
        )
    elif request.seed_content is not None:
        seed_payload = json.dumps(
            {
                "seed_id": request.seed_record.seed_id if request.seed_record else "unknown",
                "content": request.seed_content,
            },
            ensure_ascii=False,
            indent=2,
        )
        inspiration_section = dedent(
            f"""
            ## Seed Content

            Use the seed's technical intent and useful constraints as primary inspiration. The
            content may be a task description, code, configuration, logs, or mixed text. Create a
            new task rather than copying its wording, solution, tests, or completed artifacts.

            ```json
            {seed_payload}
            ```
            """
        ).strip()
        inspiration_rule = (
            "Derive the task from the seed content and inferred domain. Keep one coherent workflow; "
            "do not add unrelated objectives."
        )
    else:
        inspiration_section = dedent(
            f"""
            ## Candidate Skills

            {skills}
            """
        ).strip()
        inspiration_rule = (
            "Treat skills as inspiration; choose one anchor and up to three supporting skills. "
            "Use a supporting skill only when it directly shares an input, state, or deliverable "
            "with the anchor, or validates the anchor's result; ignore all others."
        )

    solution_output = ""
    solution_requirements = ""
    authoring_limit = dedent(
        """
        Write the package directly. Do not create a reference solution, candidate implementation,
        solved output, build, or end-to-end task solution. Do not run `pytest` or the generated
        workflow. Do not modify, delete, repoint, or symlink the global `/app`.
        """
    ).strip()
    if request.atomic_card is not None:
        solution_output = "\n        - `solution/solve.sh`"
        solution_requirements = dedent(
            """
            - Create `solution/solve.sh` as an executable Bash reference solution. It must solve the
              task from the initial workspace, must not read or copy hidden tests, and must not embed
              a precomputed visible-fixture answer when the instruction requires reusable behavior.
              Keep it outside `workspace/`; the task-solving agent will not see it.
            """
        ).strip()
        authoring_limit = dedent(
            """
            Write the unsolved package and the separate internal `solution/solve.sh` directly. Do
            not run `pytest`, the solution, or the generated workflow. Do not place solved artifacts
            in `workspace/`. Do not modify, delete, repoint, or symlink the global `/app`.
            """
        ).strip()

    prompt = dedent(
        f"""
        # terminalGen Agent Task Package Request

        Create one realistic terminal-bench-style task package in the current directory. Use one
        coherent professional objective requiring at least two meaningful stages of terminal work.

        ## Output

        Create exactly these paths relative to the current directory:

        - `task.json`
        - `instruction.md`
        - `workspace/`
        - `tests/test_outputs.py`__SOLUTION_OUTPUT__

        `task.json` must have this shape:

        ```json
        {{
          "task_id": "kebab-case-id",
          "info": {{"domain": "{manifest_domain}", "difficulty": "{request.difficulty}"}},
          "test_requirements": ["pytest"],
          "sources": []
        }}
        ```

        `instruction.md` must be self-contained, realistic, unambiguous, and must not reveal
        hidden test implementation details.

        ## Runtime and Initial Workspace

        The package `workspace/` becomes the task-solving agent's `/app`; the mapping is exact:

        - package `workspace/foo` becomes runtime `/app/foo`
        - package `workspace/data/input.csv` becomes runtime `/app/data/input.csv`

        Start `instruction.md` with `Work in /app.` and use only `/app/...` runtime paths there.
        Never mention package-authoring `workspace/...`, bare `app/...`, or accidental
        `/app/app/...` paths. Tests must use `/app` and the same paths as the instruction.
        `workspace/` contains only visible initialization inputs; every input named in the
        instruction must already exist there. Do not put tests, caches, final/answer/expected/
        golden/reference/solution artifacts there, and do not use hooks or `conftest.py` to create
        primary inputs. Record downloaded sources and their runtime path, URL, and sha256 in
        `task.json.sources`.

        ## Environment

        Tasks run in the pre-designed Docker environment `{base_image}`. List extra test packages
        in `task.json.test_requirements`.

        ## Target Domain

        __TARGET_DOMAIN__

        __INSPIRATION_SECTION__

        ## Task and Test Requirements

        - __INSPIRATION_RULE__
        - Make the initial workspace realistic and incomplete. Prefer one to three related
          deliverables and derive difficulty from diagnosis, edge cases, integration, or validation.
        - For file-processing tasks, require a reusable script, CLI, program, or configuration.
          Reject hard-coded visible outputs, generic tutorials, and unrelated
          subtasks.
        - Tests must be deterministic, run from `/app`, and verify observable behavior. Edge-case
          or negative behavior may be checked when useful, but tests do not need to create new
          inputs. Compute expected values independently and do not require unstated implementation
          details.
        - Tests must not inspect source text or ASTs, assert line ordering, or require a specific
          library/API shape unless the instruction explicitly makes that interface part of the
          user contract. Prefer exercising the command or importable behavior directly.
        __SOLUTION_REQUIREMENTS__

        ## Authoring Limit

        __AUTHORING_LIMIT__
        After writing files, perform at most one static audit: check required paths, parse
        `task.json` as JSON and `tests/test_outputs.py` with Python `ast`, and inspect path
        consistency. Do not execute tests or the workflow. If the audit finds an issue, make one
        focused fix, then stop without auditing again.

        Final answer behavior: once the files are written, stop. Do not print a summary.
        """
    ).strip()
    return (
        prompt.replace("__TARGET_DOMAIN__", target_domain)
        .replace("__INSPIRATION_SECTION__", inspiration_section)
        .replace("__INSPIRATION_RULE__", inspiration_rule)
        .replace("__SOLUTION_OUTPUT__", solution_output)
        .replace("__SOLUTION_REQUIREMENTS__", solution_requirements)
        .replace("__AUTHORING_LIMIT__", authoring_limit)
    )


def _atomic_bucket_rule(request: GenerationRequest) -> str:
    if request.variant_bucket is None:
        return "Follow the capability card without changing its required gates."
    rules = {
        "same-domain-parametric": (
            "Stay in the primary domain and vary scale, boundary conditions, values, and data "
            "distribution without changing the capability chain."
        ),
        "same-domain-structural": (
            "Stay in the primary domain and change file topology, interface, representation, or "
            "tooling while preserving the capability chain."
        ),
        "cross-domain-isomorph": (
            "Use the selected non-primary target domain and express an isomorphic constraint "
            "chain; do not reuse the source domain story."
        ),
        "diagnose-and-repair": (
            "Provide a plausible near-correct implementation or evidence trail whose observed "
            "failure must be diagnosed before a durable repair."
        ),
        "adversarial-rollback": (
            "Include at least one tempting shortcut or injected late failure and require the "
            "tests to prove failure visibility, rollback, and invariant preservation."
        ),
    }
    return rules[request.variant_bucket.value]


def build_seed_prompt(
    request: GenerationRequest,
    domains: list[DomainSpec],
    *,
    base_image: str = DEFAULT_BASE_IMAGE,
) -> str:
    if request.seed_record is None:
        raise ValueError("seed_record is required for seed-based generation")
    if request.seed_content is None:
        raise ValueError("seed_content is required for seed-based generation")
    if not domains:
        raise ValueError("at least one domain is required for seed-based generation")

    payload = {
        "seed_id": request.seed_record.seed_id,
        "content": request.seed_content,
        "allowed_domains": [domain.name for domain in domains],
        "difficulty": request.difficulty,
    }
    return dedent(
        f"""
        #Task Generation Request

        ##Seed Content

        {json.dumps(payload, ensure_ascii=False, indent=2)}

{INSPIRATION_CRITICAL_RULES.rstrip()}

        ##Pre-designed Docker Environment
        Tasks will run in this pre-designed Docker environment {base_image}
        If additional packages are needed for testing, list them in "test_requirements".

        ##Instructions
        CREATE A NOVEL TASK that:
        1. Infers the best domain from the complete seed content and uses exactly one allowed domain
        2. Uses the seed's technical intent and useful constraints as primary inspiration
        3. Does not copy a supplied solution, tests, final outputs, or completed artifacts
        4. Has one coherent professional objective with clear, unambiguous specifications
        5. Derives difficulty from reasoning, edge cases, integration, or validation

        The seed may be a task description, code, configuration, logs, or mixed text.
        Preserve useful technical substance, but create a new realistic terminal workflow rather
        than mechanically restating the seed.
        """
    ).strip()
