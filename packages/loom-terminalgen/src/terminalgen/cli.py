from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from terminalgen.agent_skills import DEFAULT_AGENT_SKILL_PLANS_PATH
from terminalgen.atomic import DEFAULT_ATOMIC_CARDS_PATH, load_atomic_weakness_cards
from terminalgen.bundle_packaging import package_validated_bundles
from terminalgen.bundle_validation import validate_bundle_tree
from terminalgen.catalog import get_domain_specs
from terminalgen.constants import (
    DEFAULT_AGENT_TIMEOUT_BY_DIFFICULTY,
    DEFAULT_BASE_IMAGE,
    DEFAULT_MAX_SAME_TASK_NAMES,
)
from terminalgen.models import Difficulty, GenerationMode, SynthesizerMode
from terminalgen.opencode_synthesizer import (
    DEFAULT_MAX_ARTIFACT_BYTES,
    DEFAULT_MAX_TOTAL_ARTIFACT_BYTES,
    OpencodeConfig,
    OpencodeTaskSynthesizer,
)
from terminalgen.openai_client import OpenAIConfig, OpenAITextGenerator
from terminalgen.pipeline import SyntheticTaskPipeline, load_seed_records
from terminalgen.planner import OpenAITaskPlanner

app = typer.Typer(help="Synthetic terminal task generation pipeline for terminus2 datasets.")


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_seed_dir() -> Path:
    return _project_root() / "seeds"


def _default_instruction_dir() -> Path | None:
    candidate = _project_root() / "terminal-bench-2"
    return candidate if candidate.exists() else None


class _UnusedTextGenerator:
    def generate_json(self, system_prompt: str, user_prompt: str) -> dict:
        raise RuntimeError("generate_json should not be called for standalone dedup")


@app.command("generate")
def generate(
    mode: GenerationMode = typer.Option(..., help="Generation mode."),
    count: int = typer.Option(..., min=1, help="Final number of accepted tasks to write."),
    model: str = typer.Option(
        ...,
        help="Model used by the selected synthesizer; in plan-first mode this is the weaker agent model.",
    ),
    planner_model: str | None = typer.Option(
        None,
        help="Optional strong OpenAI-compatible model that plans each task before an opencode agent authors it.",
    ),
    api_key: str | None = typer.Option(None, envvar="OPENAI_API_KEY", help="OpenAI API key."),
    output: Path = typer.Option(..., help="Output directory for terminal-bench-style task folders."),
    synthesizer: SynthesizerMode | None = typer.Option(
        None,
        help="Task synthesizer backend. Skill modes default to opencode-agent; seed-based defaults to openai-json.",
    ),
    seed_file: Path = typer.Option(
        _default_seed_dir(),
        help="Seed JSONL file or directory of top-level *.jsonl files for seed-based generation. Defaults to the project-root ./seeds directory.",
    ),
    atomic_cards_path: Path = typer.Option(
        DEFAULT_ATOMIC_CARDS_PATH,
        help="JSON catalog of atomic weakness cards used by atomic-target generation.",
    ),
    atomic_per_card: int | None = typer.Option(
        None,
        min=1,
        help="Required accepted task count for every atomic weakness card.",
    ),
    atomic_max_attempts_per_slot: int = typer.Option(
        3,
        min=1,
        help="Maximum complete generation attempts for each atomic quota slot.",
    ),
    base_url: str | None = typer.Option(None, envvar="OPENAI_BASE_URL", help="OpenAI-compatible base URL."),
    workers: int | None = typer.Option(
        None,
        help="Number of generation threads. Defaults to 2 for opencode-agent and 6 for openai-json.",
    ),
    temperature: float = typer.Option(1.0, min=0.0, max=2.0, help="Sampling temperature."),
    max_retries: int = typer.Option(3, min=1, help="Per-sample retry count."),
    difficulty: Difficulty = typer.Option(Difficulty.MIXED, help="Target difficulty."),
    domains: str | None = typer.Option(None, help="Comma-separated domain allowlist."),
    seed: int = typer.Option(7, help="Random seed."),
    write_batch_size: int = typer.Option(
        5,
        min=1,
        help="Retained compatibility option; directory exports are written immediately.",
    ),
    dedup_instruction_dir: Path | None = typer.Option(
        _default_instruction_dir(),
        help="Directory containing terminal-bench-style instruction.md files for post-generation n-gram dedup.",
    ),
    dedup_ngram_size: int = typer.Option(5, min=1, help="n-gram size used for prompt vs instruction dedup."),
    dedup_jaccard_threshold: float = typer.Option(
        0.55,
        min=0.0,
        max=1.0,
        help="Remove generated tasks whose prompt Jaccard similarity to instruction.md exceeds this threshold.",
    ),
    no_benchmark_dedup: bool = typer.Option(
        False,
        "--no-benchmark-dedup",
        help="Disable the final benchmark instruction.md n-gram dedup stage.",
    ),
    catalog_config: Path | None = typer.Option(
        None,
        help="Path to a catalog JSON file defining domains, weights, and skills.",
    ),
    agent_skill_plans_path: Path | None = typer.Option(
        DEFAULT_AGENT_SKILL_PLANS_PATH,
        help="Path to a JSONL file of agent skill plans used by agent-skill-based generation.",
    ),
    call_log_dir: Path | None = typer.Option(
        None,
        help="Directory for per-call OpenAI logs. Defaults to <output>/calls.",
    ),
    input_token_price_usd_per_1m: float | None = typer.Option(
        None,
        min=0.0,
        help="USD price per 1M input tokens used for cost estimation.",
    ),
    output_token_price_usd_per_1m: float | None = typer.Option(
        None,
        min=0.0,
        help="USD price per 1M output tokens used for cost estimation.",
    ),
    base_image: str = typer.Option(
        DEFAULT_BASE_IMAGE,
        help="Base Docker image used by the default task environment template and exposed to the generator.",
    ),
    agent_timeout_medium: float = typer.Option(
        DEFAULT_AGENT_TIMEOUT_BY_DIFFICULTY["medium"],
        min=0.1,
        help="Agent timeout in seconds written into task.toml for medium tasks.",
    ),
    agent_timeout_hard: float = typer.Option(
        DEFAULT_AGENT_TIMEOUT_BY_DIFFICULTY["hard"],
        min=0.1,
        help="Agent timeout in seconds written into task.toml for hard tasks.",
    ),
    agent_timeout_expert: float = typer.Option(
        DEFAULT_AGENT_TIMEOUT_BY_DIFFICULTY["expert"],
        min=0.1,
        help="Agent timeout in seconds written into task.toml for expert tasks.",
    ),
    max_same_task_names: int = typer.Option(
        DEFAULT_MAX_SAME_TASK_NAMES,
        min=1,
        help="Maximum number of exported tasks that may share the same normalized task name within a domain.",
    ),
    max_artifact_bytes: int = typer.Option(
        DEFAULT_MAX_ARTIFACT_BYTES,
        min=1,
        help="Maximum size in bytes for a single opencode-generated workspace file.",
    ),
    max_total_artifact_bytes: int = typer.Option(
        DEFAULT_MAX_TOTAL_ARTIFACT_BYTES,
        min=1,
        help="Maximum total size in bytes for an opencode-generated workspace tree.",
    ),
) -> None:
    console = Console()
    effective_synthesizer = _resolve_synthesizer(mode, synthesizer)
    effective_workers = _resolve_workers(workers, effective_synthesizer)
    selected_domains = _parse_domains(domains)
    seed_records = None
    atomic_cards = None
    if mode == GenerationMode.SEED_BASED:
        try:
            seed_records = load_seed_records(seed_file)
        except (OSError, ValueError) as exc:
            raise typer.BadParameter(str(exc), param_hint="--seed-file") from exc
    elif mode == GenerationMode.ATOMIC_TARGET:
        if effective_synthesizer != SynthesizerMode.OPENCODE_AGENT:
            raise typer.BadParameter(
                "atomic-target requires opencode-agent so it can author full task bundles",
                param_hint="--synthesizer",
            )
        if atomic_per_card is None:
            raise typer.BadParameter(
                "atomic-target requires --atomic-per-card",
                param_hint="--atomic-per-card",
            )
        try:
            atomic_cards = load_atomic_weakness_cards(atomic_cards_path)
        except (OSError, ValueError) as exc:
            raise typer.BadParameter(str(exc), param_hint="--atomic-cards-path") from exc
    effective_call_log_dir = call_log_dir or (output / "calls")

    if mode == GenerationMode.SEED_BASED and not seed_records:
        raise typer.BadParameter(
            f"no seed records found under {seed_file}",
            param_hint="--seed-file",
        )

    planner = None
    if planner_model is not None:
        if effective_synthesizer != SynthesizerMode.OPENCODE_AGENT:
            raise typer.BadParameter(
                "planner model is only supported with opencode-agent",
                param_hint="--planner-model",
            )
        if not api_key:
            raise typer.BadParameter(
                "api key is required when planner model is set",
                param_hint="--api-key",
            )
        planner = OpenAITaskPlanner(
            OpenAITextGenerator(
                OpenAIConfig(
                    model=planner_model,
                    api_key=api_key,
                    base_url=base_url,
                    temperature=temperature,
                    max_retries=max_retries,
                    call_log_dir=effective_call_log_dir / "planner",
                    input_token_price_usd_per_1m=input_token_price_usd_per_1m,
                    output_token_price_usd_per_1m=output_token_price_usd_per_1m,
                )
            )
        )

    if effective_synthesizer == SynthesizerMode.OPENAI_JSON:
        if not api_key:
            raise typer.BadParameter("api key is required for openai-json", param_hint="--api-key")
        generator = OpenAITextGenerator(
            OpenAIConfig(
                model=model,
                api_key=api_key,
                base_url=base_url,
                temperature=temperature,
                max_retries=max_retries,
                call_log_dir=effective_call_log_dir,
                input_token_price_usd_per_1m=input_token_price_usd_per_1m,
                output_token_price_usd_per_1m=output_token_price_usd_per_1m,
            )
        )
    else:
        generator = OpencodeTaskSynthesizer(
            OpencodeConfig(
                model=model,
                call_log_dir=effective_call_log_dir,
                staging_dir=output / ".staging",
                max_retries=max_retries,
                max_artifact_bytes=max_artifact_bytes,
                max_total_artifact_bytes=max_total_artifact_bytes,
            )
        )
    pipeline = SyntheticTaskPipeline(generator, console=console, random_seed=seed)
    preserve_output_dirs = _preserve_output_dirs(output, effective_call_log_dir)
    agent_timeout_by_difficulty = {
        "medium": agent_timeout_medium,
        "hard": agent_timeout_hard,
        "expert": agent_timeout_expert,
    }

    with console.status("[bold cyan]Generating synthetic terminal tasks...", spinner="dots"):
        tasks = pipeline.generate_tasks(
            mode=mode,
            count=count,
            output_path=output,
            workers=effective_workers,
            difficulty=difficulty,
            write_batch_size=write_batch_size,
            dedup_instruction_dir=dedup_instruction_dir,
            dedup_ngram_size=dedup_ngram_size,
            dedup_jaccard_threshold=dedup_jaccard_threshold,
            benchmark_dedup_enabled=not no_benchmark_dedup,
            domains=selected_domains,
            seed_records=seed_records,
            planner=planner,
            catalog_config=catalog_config,
            agent_skill_plans_path=agent_skill_plans_path,
            base_image=base_image,
            agent_timeout_by_difficulty=agent_timeout_by_difficulty,
            max_same_task_names=max_same_task_names,
            preserve_output_dirs=preserve_output_dirs,
            atomic_cards=atomic_cards,
            atomic_per_card=atomic_per_card,
            atomic_max_attempts_per_slot=atomic_max_attempts_per_slot,
        )

    console.print(f"[bold green]Wrote {len(tasks)} task(s)[/bold green] to {output}")
    stats = generator.stats_snapshot()
    if effective_synthesizer == SynthesizerMode.OPENAI_JSON:
        cost_display = (
            f"${stats['estimated_cost_usd']:.6f}"
            if stats["cost_tracking_enabled"]
            else "n/a"
        )
        console.print(
            "[cyan]openai usage[/cyan] "
            f"calls={stats['call_count']} "
            f"input_tokens={stats['input_tokens']} "
            f"output_tokens={stats['output_tokens']} "
            f"total_tokens={stats['total_tokens']} "
            f"estimated_cost={cost_display} "
            f"logs={effective_call_log_dir}"
        )
    else:
        console.print(
            "[cyan]opencode usage[/cyan] "
            f"calls={stats['call_count']} "
            f"failed_calls={stats['failed_calls']} "
            f"accepted_packages={stats['accepted_packages']} "
            f"logs={effective_call_log_dir}"
        )
    if planner is not None:
        planner_stats = planner.stats_snapshot()
        planner_cost_display = (
            f"${planner_stats['estimated_cost_usd']:.6f}"
            if planner_stats["cost_tracking_enabled"]
            else "n/a"
        )
        console.print(
            "[cyan]planner usage[/cyan] "
            f"calls={planner_stats['call_count']} "
            f"input_tokens={planner_stats['input_tokens']} "
            f"output_tokens={planner_stats['output_tokens']} "
            f"total_tokens={planner_stats['total_tokens']} "
            f"estimated_cost={planner_cost_display} "
            f"logs={effective_call_log_dir / 'planner'}"
        )


@app.command("validate-bundles")
def validate_bundles(
    tasks: Path = typer.Option(
        ...,
        "--tasks",
        help="Root containing generated terminal-bench task bundle directories.",
    ),
    run_docker: bool = typer.Option(
        False,
        "--run-docker/--static-only",
        help="Build and execute every bundle in addition to static validation.",
    ),
    platform: str = typer.Option(
        "linux/arm64",
        help="Docker platform used for build and execution validation.",
    ),
    solution_repetitions: int = typer.Option(
        2,
        min=1,
        help="Number of fresh-container reference solution executions per bundle.",
    ),
    docker_timeout_sec: float = typer.Option(
        1800.0,
        min=1.0,
        help="Timeout for each Docker build or container execution.",
    ),
) -> None:
    console = Console()
    try:
        results = validate_bundle_tree(
            tasks,
            run_docker=run_docker,
            platform=platform,
            solution_repetitions=solution_repetitions,
            docker_timeout_sec=docker_timeout_sec,
        )
    except (OSError, ValueError) as exc:
        raise typer.BadParameter(str(exc), param_hint="--tasks") from exc
    passed = sum(result.passed for result in results)
    failed = len(results) - passed
    console.print(
        f"[bold {'green' if failed == 0 else 'red'}]Bundle validation complete[/bold "
        f"{'green' if failed == 0 else 'red'}] "
        f"passed={passed} failed={failed} tasks={tasks}"
    )
    if not results:
        console.print("[bold red]No task bundles found[/bold red]")
        raise typer.Exit(code=1)
    if failed:
        for result in results:
            if not result.passed:
                console.print(f"[red]{result.task_path}[/red]: {'; '.join(result.errors)}")
        raise typer.Exit(code=1)


@app.command("package-bundles")
def package_bundles(
    tasks: Path = typer.Option(
        ...,
        "--tasks",
        help="Validated task bundle root.",
    ),
    output: Path = typer.Option(
        ...,
        "--output",
        help="Empty output directory for deterministic zip shards.",
    ),
    shard_size: int = typer.Option(
        100,
        min=1,
        help="Maximum number of task bundles in each zip shard.",
    ),
    include_solutions: bool = typer.Option(
        True,
        "--include-solutions/--exclude-solutions",
        help="Include reference solutions in delivery shards.",
    ),
    require_docker_validation: bool = typer.Option(
        True,
        "--require-docker-validation/--allow-static-only",
        help="Require every packaged bundle to have passed Docker validation.",
    ),
) -> None:
    console = Console()
    try:
        manifest = package_validated_bundles(
            tasks,
            output,
            shard_size=shard_size,
            include_solutions=include_solutions,
            require_docker_validation=require_docker_validation,
        )
    except (OSError, ValueError) as exc:
        raise typer.BadParameter(str(exc), param_hint="--tasks") from exc
    console.print(
        "[bold green]Bundle packaging complete[/bold green] "
        f"tasks={manifest['task_count']} shards={manifest['shard_count']} output={output}"
    )


@app.command("dedup")
def dedup(
    tasks: Path = typer.Option(
        ...,
        "--tasks",
        help="Task directory containing terminal-bench-style task folders to deduplicate in place.",
    ),
    dedup_instruction_dir: Path | None = typer.Option(
        _default_instruction_dir(),
        help="Directory containing terminal-bench-style instruction.md files used as the dedup benchmark set.",
    ),
    dedup_ngram_size: int = typer.Option(5, min=1, help="n-gram size used for prompt vs instruction dedup."),
    dedup_jaccard_threshold: float = typer.Option(
        0.55,
        min=0.0,
        max=1.0,
        help="Remove exported tasks whose prompt Jaccard similarity to instruction.md exceeds this threshold.",
    ),
) -> None:
    console = Console()
    pipeline = SyntheticTaskPipeline(_UnusedTextGenerator(), console=console)

    with console.status("[bold cyan]Deduplicating exported terminal tasks...", spinner="dots"):
        kept_task_dirs, removed_task_dirs = pipeline.deduplicate_output_tasks(
            output_path=tasks,
            instruction_dir=dedup_instruction_dir,
            ngram_size=dedup_ngram_size,
            jaccard_threshold=dedup_jaccard_threshold,
        )

    console.print(
        "[bold green]Dedup complete[/bold green] "
        f"kept={len(kept_task_dirs)} removed={len(removed_task_dirs)} tasks={tasks}"
    )


@app.command("show-domains")
def show_domains(
    catalog_config: Path | None = typer.Option(
        None,
        help="Path to a catalog JSON file defining domains, weights, and skills.",
    ),
) -> None:
    console = Console()
    table = Table(title="terminalGen Domains")
    table.add_column("Domain", style="cyan")
    table.add_column("Weight", style="green", justify="right")
    table.add_column("Summary", style="white")
    table.add_column("Skills", style="magenta")

    for domain in get_domain_specs(config_path=catalog_config):
        table.add_row(
            domain.name,
            f"{domain.weight:g}",
            domain.summary,
            ", ".join(domain.skills),
        )
    console.print(table)


def _parse_domains(value: str | None) -> list[str] | None:
    if value is None:
        return None
    domains = [item.strip() for item in value.split(",") if item.strip()]
    return domains or None


def _resolve_synthesizer(
    mode: GenerationMode,
    synthesizer: SynthesizerMode | None,
) -> SynthesizerMode:
    if synthesizer is not None:
        return synthesizer
    if mode in {
        GenerationMode.SKILL_BASED,
        GenerationMode.AGENT_SKILL_BASED,
        GenerationMode.ATOMIC_TARGET,
    }:
        return SynthesizerMode.OPENCODE_AGENT
    return SynthesizerMode.OPENAI_JSON


def _resolve_workers(workers: int | None, synthesizer: SynthesizerMode) -> int:
    if workers is not None:
        if workers <= 0:
            raise typer.BadParameter("workers must be > 0", param_hint="--workers")
        return workers
    if synthesizer == SynthesizerMode.OPENCODE_AGENT:
        return 2
    return 6


def _preserve_output_dirs(output: Path, call_log_dir: Path | None) -> list[str]:
    if call_log_dir is None:
        return []
    try:
        relative = call_log_dir.relative_to(output)
    except ValueError:
        return []
    if not relative.parts:
        return []
    return [relative.parts[0]]


if __name__ == "__main__":
    app()
