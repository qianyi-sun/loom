from __future__ import annotations

import hashlib
import json
import math
import random
import re
import shutil
import threading
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Mapping, Protocol, TypeVar

from rich.console import Console

from terminalgen.agent_skills import DEFAULT_AGENT_SKILL_PLANS_PATH, load_agent_skill_catalog
from terminalgen.atomic import build_atomic_generation_requests
from terminalgen.bundle_validation import (
    write_atomic_batch_contract,
    write_generation_manifest,
)
from terminalgen.catalog import DomainSpec, get_domain_spec, get_domain_specs
from terminalgen.constants import DEFAULT_BASE_IMAGE, DEFAULT_MAX_SAME_TASK_NAMES
from terminalgen.models import (
    AtomicWeaknessCard,
    DatasetTask,
    Difficulty,
    GenerationMode,
    GenerationRequest,
    SeedRecord,
    TaskPlan,
)
from terminalgen.planner import TaskPlanner
from terminalgen.prompts import (
    build_agent_task_prompt,
    build_seed_prompt,
    build_seed_system_prompt,
    build_skill_prompt,
    build_skill_system_prompt,
)
from terminalgen.terminal_bench_export import (
    DuplicateTaskNameLimitError,
    export_task,
    prune_tasks,
)


class TextGenerator(Protocol):
    def generate_json(self, system_prompt: str, user_prompt: str) -> dict: ...


T = TypeVar("T")


class SyntheticTaskPipeline:
    def __init__(
        self,
        generator: TextGenerator,
        *,
        console: Console | None = None,
        random_seed: int = 7,
    ) -> None:
        self.generator = generator
        self.console = console or Console()
        self.random = random.Random(random_seed)
        self._seen_hashes: set[str] = set()
        self._lock = threading.Lock()

    def generate_tasks(
        self,
        *,
        mode: GenerationMode,
        count: int,
        output_path: Path,
        workers: int,
        difficulty: Difficulty,
        write_batch_size: int = 5,
        dedup_instruction_dir: Path | None = None,
        dedup_ngram_size: int = 5,
        dedup_jaccard_threshold: float = 0.55,
        benchmark_dedup_enabled: bool = True,
        domains: list[str] | None = None,
        seed_records: list[SeedRecord] | None = None,
        planner: TaskPlanner | None = None,
        catalog_config: Path | None = None,
        agent_skill_plans_path: Path | None = DEFAULT_AGENT_SKILL_PLANS_PATH,
        base_image: str = DEFAULT_BASE_IMAGE,
        agent_timeout_by_difficulty: Mapping[str, float] | None = None,
        max_same_task_names: int = DEFAULT_MAX_SAME_TASK_NAMES,
        preserve_output_dirs: list[str] | None = None,
        atomic_cards: list[AtomicWeaknessCard] | None = None,
        atomic_per_card: int | None = None,
        atomic_max_attempts_per_slot: int = 3,
    ) -> list[DatasetTask]:
        if count <= 0:
            raise ValueError("count must be > 0")
        if write_batch_size <= 0:
            raise ValueError("write_batch_size must be > 0")
        if dedup_ngram_size <= 0:
            raise ValueError("dedup_ngram_size must be > 0")
        if not 0.0 <= dedup_jaccard_threshold <= 1.0:
            raise ValueError("dedup_jaccard_threshold must be between 0 and 1")
        if max_same_task_names <= 0:
            raise ValueError("max_same_task_names must be > 0")
        if atomic_max_attempts_per_slot <= 0:
            raise ValueError("atomic_max_attempts_per_slot must be > 0")
        if mode == GenerationMode.SEED_BASED and not seed_records:
            raise ValueError("seed_records are required for seed-based generation")
        if mode == GenerationMode.ATOMIC_TARGET:
            if not atomic_cards:
                raise ValueError("atomic_cards are required for atomic-target generation")
            if atomic_per_card is None or atomic_per_card <= 0:
                raise ValueError("atomic_per_card must be > 0 for atomic-target generation")
            expected_count = len(atomic_cards) * atomic_per_card
            if count != expected_count:
                raise ValueError(
                    f"atomic-target count must equal cards * per-card ({expected_count})"
                )
            return self._generate_atomic_tasks(
                cards=atomic_cards,
                per_card_count=atomic_per_card,
                output_path=output_path,
                workers=workers,
                difficulty=difficulty,
                catalog_config=catalog_config,
                planner=planner,
                base_image=base_image,
                agent_timeout_by_difficulty=agent_timeout_by_difficulty,
                max_same_task_names=max_same_task_names,
                max_attempts_per_slot=atomic_max_attempts_per_slot,
                dedup_instruction_dir=dedup_instruction_dir,
                dedup_ngram_size=dedup_ngram_size,
                dedup_jaccard_threshold=dedup_jaccard_threshold,
                benchmark_dedup_enabled=benchmark_dedup_enabled,
                preserve_output_dirs=preserve_output_dirs,
            )

        accepted: list[DatasetTask] = []
        if output_path.exists():
            shutil.rmtree(output_path)
        output_path.mkdir(parents=True, exist_ok=True)
        exported_task_dirs_by_identity: dict[int, str] = {}
        used_task_dir_names: set[str] = set()

        max_workers = max(1, workers)
        max_attempts = max(math.ceil(count * 1.2), count)
        executor = ThreadPoolExecutor(max_workers=max_workers)
        future_map: dict[Future[DatasetTask], GenerationRequest] = {}
        attempts_submitted = 0
        stopped_early = False
        ordered_seed_records = list(seed_records or [])
        if mode == GenerationMode.SEED_BASED:
            self.random.shuffle(ordered_seed_records)
        seed_cursor = 0

        def submit_request() -> bool:
            nonlocal attempts_submitted, seed_cursor
            if attempts_submitted >= max_attempts:
                return False
            spec = self._build_generation_request(
                sample_index=attempts_submitted,
                mode=mode,
                difficulty=difficulty,
                domains=domains,
                seed_records=ordered_seed_records,
                seed_index=seed_cursor,
                catalog_config=catalog_config,
                agent_skill_plans_path=agent_skill_plans_path,
            )
            if spec.generation_mode == GenerationMode.SEED_BASED:
                seed_cursor += 1
            attempts_submitted += 1
            future_map[
                executor.submit(self._generate_one, spec, catalog_config, base_image, planner)
            ] = spec
            return True

        def top_up_requests() -> None:
            target_inflight = min(max_workers, count - len(accepted))
            while len(future_map) < target_inflight:
                if not submit_request():
                    break

        try:
            top_up_requests()

            while future_map:
                future = next(as_completed(list(future_map)))
                spec = future_map.pop(future)
                try:
                    task = future.result()
                except Exception as exc:  # noqa: BLE001
                    self.console.print(
                        f"[yellow]generation failed[/yellow] sample={spec.sample_index} "
                        f"mode={spec.generation_mode.value} domain={spec.domain}: {exc}"
                    )
                else:
                    accepted_task, rejection_reason = self._accept(task)
                    if accepted_task:
                        try:
                            task_dir_name = export_task(
                                output_path,
                                task,
                                used_names=used_task_dir_names,
                                base_image=base_image,
                                agent_timeout_by_difficulty=agent_timeout_by_difficulty,
                                max_same_task_names=max_same_task_names,
                            )
                        except DuplicateTaskNameLimitError as exc:
                            self.console.print(
                                f"[yellow]filtered[/yellow] {task.stable_id} due to {exc}"
                            )
                        else:
                            accepted.append(task)
                            exported_task_dirs_by_identity[id(task)] = task_dir_name
                            self.console.print(
                                f"[green]accepted[/green] {task.stable_id} "
                                f"({len(accepted)}/{count})"
                            )
                    else:
                        self.console.print(
                            f"[yellow]filtered[/yellow] {task.stable_id} due to {rejection_reason}"
                        )

                if len(accepted) >= count:
                    stopped_early = True
                    for pending in future_map:
                        pending.cancel()
                    break

                top_up_requests()
        finally:
            executor.shutdown(wait=not stopped_early, cancel_futures=stopped_early)

        if len(accepted) < count and attempts_submitted >= max_attempts:
            self.console.print(
                f"[yellow]attempt budget exhausted[/yellow] accepted={len(accepted)} "
                f"requested={count} attempts={attempts_submitted}"
            )

        accepted = accepted[:count]
        if benchmark_dedup_enabled and dedup_instruction_dir is not None:
            accepted = self._deduplicate_against_instruction_dir(
                accepted,
                instruction_dir=dedup_instruction_dir,
                ngram_size=dedup_ngram_size,
                jaccard_threshold=dedup_jaccard_threshold,
            )
        elif not benchmark_dedup_enabled:
            self.console.print("[cyan]dedup skipped[/cyan] benchmark n-gram dedup disabled")
        keep_task_dirs = {
            exported_task_dirs_by_identity[id(task)]
            for task in accepted
            if id(task) in exported_task_dirs_by_identity
        }
        prune_tasks(
            output_path,
            keep_task_dirs=keep_task_dirs,
            preserve_dirs=set(preserve_output_dirs or []),
        )
        return accepted

    def deduplicate_output_tasks(
        self,
        *,
        output_path: Path,
        instruction_dir: Path | None,
        ngram_size: int = 5,
        jaccard_threshold: float = 0.55,
    ) -> tuple[list[str], list[str]]:
        if ngram_size <= 0:
            raise ValueError("dedup_ngram_size must be > 0")
        if not 0.0 <= jaccard_threshold <= 1.0:
            raise ValueError("dedup_jaccard_threshold must be between 0 and 1")
        if not output_path.exists():
            self.console.print(f"[yellow]dedup skipped[/yellow] output dir not found: {output_path}")
            return [], []

        exported_tasks = self._load_exported_tasks(output_path)
        if not exported_tasks:
            self.console.print(
                f"[yellow]dedup skipped[/yellow] no exported task directories found under {output_path}"
            )
            return [], []
        if instruction_dir is None:
            self.console.print("[yellow]dedup skipped[/yellow] no instruction dir configured")
            return [task_dir for task_dir, _prompt in exported_tasks], []

        all_task_dirs = [task_dir for task_dir, _prompt in exported_tasks]
        kept_task_dirs = self._deduplicate_entries_against_instruction_dir(
            [(task_dir, prompt, task_dir) for task_dir, prompt in exported_tasks],
            instruction_dir=instruction_dir,
            ngram_size=ngram_size,
            jaccard_threshold=jaccard_threshold,
        )
        kept_set = set(kept_task_dirs)
        removed_task_dirs = [task_dir for task_dir in all_task_dirs if task_dir not in kept_set]

        for task_dir in removed_task_dirs:
            task_path = output_path / task_dir
            if task_path.exists():
                shutil.rmtree(task_path)
                self._remove_empty_parent_dirs(task_path.parent, stop_dir=output_path)
        return kept_task_dirs, removed_task_dirs

    def _build_generation_requests(
        self,
        *,
        mode: GenerationMode,
        count: int,
        difficulty: Difficulty,
        domains: list[str] | None,
        seed_records: list[SeedRecord],
        catalog_config: Path | None,
        agent_skill_plans_path: Path | None = DEFAULT_AGENT_SKILL_PLANS_PATH,
    ) -> list[GenerationRequest]:
        ordered_seed_records = list(seed_records)
        if mode == GenerationMode.SEED_BASED:
            self.random.shuffle(ordered_seed_records)
        requests: list[GenerationRequest] = []
        seed_cursor = 0
        for sample_index in range(count):
            request = self._build_generation_request(
                sample_index=sample_index,
                mode=mode,
                difficulty=difficulty,
                domains=domains,
                seed_records=ordered_seed_records,
                seed_index=seed_cursor,
                catalog_config=catalog_config,
                agent_skill_plans_path=agent_skill_plans_path,
            )
            requests.append(request)
            if request.generation_mode == GenerationMode.SEED_BASED:
                seed_cursor += 1
        return requests

    def _build_generation_request(
        self,
        *,
        sample_index: int,
        mode: GenerationMode,
        difficulty: Difficulty,
        domains: list[str] | None,
        seed_records: list[SeedRecord],
        catalog_config: Path | None,
        seed_index: int | None = None,
        agent_skill_plans_path: Path | None = DEFAULT_AGENT_SKILL_PLANS_PATH,
    ) -> GenerationRequest:
        domain_specs = get_domain_specs(domains, config_path=catalog_config)
        difficulty_value = self._sample_difficulty(difficulty)
        if _is_skill_generation_mode(mode):
            domain = self._sample_domain(domain_specs)
            skills = (
                self._sample_agent_domain_skills(domain, agent_skill_plans_path)
                if mode == GenerationMode.AGENT_SKILL_BASED
                else self._sample_domain_skills(domain)
            )
            return GenerationRequest(
                sample_index=sample_index,
                generation_mode=mode,
                domain=domain.name,
                difficulty=difficulty_value,
                skills=skills,
            )

        if not seed_records:
            raise ValueError("seed records are required for seed-based generation")
        record_index = sample_index if seed_index is None else seed_index
        seed_record = seed_records[record_index % len(seed_records)]
        return GenerationRequest(
            sample_index=sample_index,
            generation_mode=mode,
            domain="auto",
            difficulty=difficulty_value,
            seed_record=seed_record,
            seed_content=seed_record.content,
            domain_candidates=[domain.name for domain in domain_specs],
        )

    def _sample_difficulty(self, difficulty: Difficulty) -> str:
        if difficulty == Difficulty.MIXED:
            return self.random.choice(
                [
                    Difficulty.MEDIUM.value,
                    Difficulty.HARD.value,
                    Difficulty.EXPERT.value,
                ]
            )
        return difficulty.value

    def _sample_domain(self, domain_specs: list[DomainSpec]) -> DomainSpec:
        if not domain_specs:
            raise ValueError("at least one domain must be available for sampling")
        weights = [domain.weight for domain in domain_specs]
        if not any(weight > 0 for weight in weights):
            raise ValueError("selected domains must include at least one positive sampling weight")
        return self.random.choices(domain_specs, weights=weights, k=1)[0]

    def _sample_domain_skills(
        self,
        domain: DomainSpec,
    ) -> list[str]:
        available = list(domain.skills)
        return self._sample_skills(available, domain.name)

    def _sample_agent_domain_skills(
        self,
        domain: DomainSpec,
        agent_skill_plans_path: Path | None,
    ) -> list[str]:
        catalog = load_agent_skill_catalog(agent_skill_plans_path)
        available = list(catalog.skills_for_domain(domain.name))
        return self._sample_skills(available, domain.name)

    def _sample_skills(self, available: list[str], domain_name: str) -> list[str]:
        if not available:
            raise ValueError(f"domain {domain_name!r} does not define any skills")
        chosen: list[str] = []
        min_size = min(3, len(available))
        max_size = min(5, len(available))
        target_size = self.random.randint(min_size, max_size)

        remaining = [skill for skill in available if skill not in chosen]
        while len(chosen) < target_size and remaining:
            skill = self.random.choice(remaining)
            chosen.append(skill)
            remaining.remove(skill)
        return chosen

    def _generate_one(
        self,
        spec: GenerationRequest,
        catalog_config: Path | None,
        base_image: str,
        planner: TaskPlanner | None = None,
    ) -> DatasetTask:
        is_agent_generator = hasattr(self.generator, "generate_task")
        if planner is not None and not is_agent_generator:
            raise ValueError("task planning is only supported with an agent generator")
        is_seed_generation = spec.generation_mode == GenerationMode.SEED_BASED
        is_atomic_generation = spec.generation_mode == GenerationMode.ATOMIC_TARGET
        if is_seed_generation:
            domain_choices = get_domain_specs(spec.domain_candidates, config_path=catalog_config)
            domain = None
        elif is_atomic_generation:
            domain_choices = get_domain_specs(spec.domain_candidates, config_path=catalog_config)
            domain = get_domain_spec(spec.domain, config_path=catalog_config)
        else:
            domain_choices = []
            domain = get_domain_spec(spec.domain, config_path=catalog_config)

        if planner is not None:
            planning_domains = domain_choices if (is_seed_generation or is_atomic_generation) else [domain]
            assert all(item is not None for item in planning_domains)
            plan = planner.generate_plan(
                spec,
                [item for item in planning_domains if item is not None],
                base_image=base_image,
            )
            _validate_task_plan(plan, spec, [item for item in planning_domains if item is not None])
            spec = spec.model_copy(update={"domain": plan.domain, "plan": plan})
            domain = get_domain_spec(plan.domain, config_path=catalog_config)

        if is_atomic_generation and not is_agent_generator:
            raise ValueError("atomic-target generation requires the opencode-agent synthesizer")
        if is_agent_generator:
            system_prompt = ""
            prompt = build_agent_task_prompt(
                spec,
                domain,
                base_image=base_image,
                domain_choices=domain_choices,
            )
        elif _is_skill_generation_mode(spec.generation_mode):
            assert domain is not None
            system_prompt = build_skill_system_prompt(domain, base_image=base_image)
            prompt = build_skill_prompt(spec, domain, base_image=base_image)
        else:
            system_prompt = build_seed_system_prompt(domain_choices, base_image=base_image)
            prompt = build_seed_prompt(spec, domain_choices, base_image=base_image)
        if is_agent_generator:
            task = self.generator.generate_task(
                spec=spec,
                domain=domain,
                system_prompt=system_prompt,
                user_prompt=prompt,
                base_image=base_image,
            )
        else:
            payload = self.generator.generate_json(system_prompt, prompt)
            task = DatasetTask.model_validate(payload)
        if not task.task_id and task.id:
            task.task_id = task.id
        resolved_domain = (
            _resolve_seed_domain(task, spec.domain_candidates)
            if is_seed_generation
            else spec.domain
        )
        if spec.plan is not None and resolved_domain != spec.plan.domain:
            raise ValueError(
                f"agent task domain {resolved_domain!r} does not match planned domain "
                f"{spec.plan.domain!r}"
            )
        task.info = {
            **(task.info or {}),
            "domain": resolved_domain,
            "difficulty": spec.difficulty,
        }
        task.extra = {
            **task.extra,
            "domain": resolved_domain,
            "generation_mode": spec.generation_mode.value,
            "difficulty": spec.difficulty,
        }
        if spec.seed_record is not None:
            task.extra["seed_id"] = spec.seed_record.seed_id
        if spec.skills:
            task.extra["skills"] = spec.skills
        if spec.atomic_card is not None:
            if task.solution is None:
                raise ValueError("atomic-target task must include solution/solve.sh")
            assert spec.variant_bucket is not None
            assert spec.variant_index is not None
            task.id = None
            task.task_id = (
                f"atomic-{spec.atomic_card.capability_id}-{spec.variant_bucket.value}-"
                f"{spec.variant_index:04d}"
            )
            task.extra.update(
                {
                    "source_task": spec.atomic_card.source_task,
                    "capability_id": spec.atomic_card.capability_id,
                    "variant_bucket": spec.variant_bucket.value if spec.variant_bucket else None,
                    "variant_index": spec.variant_index,
                    "template_family_id": spec.template_family_id,
                    "attempt_index": spec.attempt_index,
                }
            )
        return task

    def _generate_atomic_tasks(
        self,
        *,
        cards: list[AtomicWeaknessCard],
        per_card_count: int,
        output_path: Path,
        workers: int,
        difficulty: Difficulty,
        catalog_config: Path | None,
        planner: TaskPlanner | None,
        base_image: str,
        agent_timeout_by_difficulty: Mapping[str, float] | None,
        max_same_task_names: int,
        max_attempts_per_slot: int,
        dedup_instruction_dir: Path | None,
        dedup_ngram_size: int,
        dedup_jaccard_threshold: float,
        benchmark_dedup_enabled: bool,
        preserve_output_dirs: list[str] | None,
    ) -> list[DatasetTask]:
        if output_path.exists():
            shutil.rmtree(output_path)
        output_path.mkdir(parents=True, exist_ok=True)
        requests = build_atomic_generation_requests(
            cards,
            per_card_count=per_card_count,
            difficulty=difficulty,
            random_seed=self.random.randint(0, 2**31 - 1),
        )
        write_atomic_batch_contract(output_path, requests)
        pending = deque(requests)
        accepted: list[DatasetTask] = []
        used_task_dir_names: set[str] = set()
        batch_prompt_ngrams: list[tuple[str, set[str]]] = []
        benchmark_refs: list[tuple[Path, set[str]]] = []
        if benchmark_dedup_enabled and dedup_instruction_dir is not None:
            if dedup_instruction_dir.exists():
                benchmark_refs = self._load_instruction_ngrams(
                    dedup_instruction_dir,
                    dedup_ngram_size,
                )
            else:
                self.console.print(
                    "[yellow]dedup skipped[/yellow] instruction dir not found: "
                    f"{dedup_instruction_dir}"
                )
        elif not benchmark_dedup_enabled:
            self.console.print("[cyan]dedup skipped[/cyan] benchmark n-gram dedup disabled")

        max_workers = max(1, workers)
        executor = ThreadPoolExecutor(max_workers=max_workers)
        future_map: dict[Future[DatasetTask], GenerationRequest] = {}
        exhausted: list[GenerationRequest] = []

        def top_up_requests() -> None:
            while pending and len(future_map) < max_workers:
                spec = pending.popleft()
                future_map[
                    executor.submit(
                        self._generate_one,
                        spec,
                        catalog_config,
                        base_image,
                        planner,
                    )
                ] = spec

        try:
            top_up_requests()
            while future_map:
                future = next(as_completed(list(future_map)))
                spec = future_map.pop(future)
                rejection_reason: str | None = None
                try:
                    task = future.result()
                except Exception as exc:  # noqa: BLE001
                    rejection_reason = f"generation failed: {exc}"
                else:
                    accepted_task, rejection_reason = self._accept_atomic_task(
                        task,
                        benchmark_refs=benchmark_refs,
                        batch_prompt_ngrams=batch_prompt_ngrams,
                        ngram_size=dedup_ngram_size,
                        jaccard_threshold=dedup_jaccard_threshold,
                    )
                    if accepted_task:
                        try:
                            task_dir_name = export_task(
                                output_path,
                                task,
                                used_names=used_task_dir_names,
                                base_image=base_image,
                                agent_timeout_by_difficulty=agent_timeout_by_difficulty,
                                max_same_task_names=max_same_task_names,
                            )
                        except DuplicateTaskNameLimitError as exc:
                            rejection_reason = str(exc)
                        else:
                            task.extra["bundle_path"] = task_dir_name
                            accepted.append(task)
                            batch_prompt_ngrams.append(
                                (task_dir_name, _text_ngrams(task.prompt, dedup_ngram_size))
                            )
                            self.console.print(
                                f"[green]accepted atomic slot[/green] {task.task_id} "
                                f"({len(accepted)}/{len(requests)})"
                            )
                            rejection_reason = None

                if rejection_reason is not None:
                    self.console.print(
                        f"[yellow]atomic slot retry[/yellow] family={spec.template_family_id} "
                        f"attempt={spec.attempt_index}/{max_attempts_per_slot}: "
                        f"{rejection_reason}"
                    )
                    if spec.attempt_index < max_attempts_per_slot:
                        pending.append(
                            spec.model_copy(update={"attempt_index": spec.attempt_index + 1})
                        )
                    else:
                        exhausted.append(spec)
                top_up_requests()
        finally:
            executor.shutdown(wait=True, cancel_futures=False)

        keep_task_dirs = {
            str(task.extra["bundle_path"])
            for task in accepted
            if task.extra.get("bundle_path")
        }
        prune_tasks(
            output_path,
            keep_task_dirs=keep_task_dirs,
            preserve_dirs=set(preserve_output_dirs or []),
        )
        write_generation_manifest(output_path, accepted)
        if exhausted:
            examples = ", ".join(
                str(spec.template_family_id) for spec in exhausted[:5]
            )
            raise RuntimeError(
                f"atomic-target quota incomplete: accepted={len(accepted)} "
                f"required={len(requests)} exhausted={len(exhausted)} examples={examples}"
            )
        return accepted

    def _accept_atomic_task(
        self,
        task: DatasetTask,
        *,
        benchmark_refs: list[tuple[Path, set[str]]],
        batch_prompt_ngrams: list[tuple[str, set[str]]],
        ngram_size: int,
        jaccard_threshold: float,
    ) -> tuple[bool, str | None]:
        if task.solution is None:
            return False, "missing reference solution"
        accepted, reason = self._accept(task)
        if not accepted:
            return False, reason
        task_ngrams = _text_ngrams(task.prompt, ngram_size)
        for ref_path, ref_ngrams in benchmark_refs:
            score = _jaccard(task_ngrams, ref_ngrams)
            if score >= jaccard_threshold:
                return False, f"near benchmark instruction score={score:.3f} ref={ref_path}"
        for label, prior_ngrams in batch_prompt_ngrams:
            score = _jaccard(task_ngrams, prior_ngrams)
            if score >= jaccard_threshold:
                return False, f"near generated instruction score={score:.3f} ref={label}"
        return True, None

    def _accept(self, task: DatasetTask) -> tuple[bool, str | None]:
        digest = hashlib.sha256(_normalize_prompt(task.prompt).encode("utf-8")).hexdigest()
        with self._lock:
            if digest in self._seen_hashes:
                return False, "duplicate prompt"
            self._seen_hashes.add(digest)
        return True, None

    def _deduplicate_against_instruction_dir(
        self,
        tasks: list[DatasetTask],
        *,
        instruction_dir: Path,
        ngram_size: int,
        jaccard_threshold: float,
    ) -> list[DatasetTask]:
        return self._deduplicate_entries_against_instruction_dir(
            [(task.stable_id or "unknown", task.prompt, task) for task in tasks],
            instruction_dir=instruction_dir,
            ngram_size=ngram_size,
            jaccard_threshold=jaccard_threshold,
        )

    def _deduplicate_entries_against_instruction_dir(
        self,
        entries: list[tuple[str, str, T]],
        *,
        instruction_dir: Path,
        ngram_size: int,
        jaccard_threshold: float,
    ) -> list[T]:
        if not instruction_dir.exists():
            self.console.print(
                f"[yellow]dedup skipped[/yellow] instruction dir not found: {instruction_dir}"
            )
            return [entry for _label, _prompt, entry in entries]

        instruction_refs = self._load_instruction_ngrams(instruction_dir, ngram_size)
        if not instruction_refs:
            self.console.print(
                f"[yellow]dedup skipped[/yellow] no instruction.md files found under {instruction_dir}"
            )
            return [entry for _label, _prompt, entry in entries]

        kept: list[T] = []
        removed = 0
        for label, prompt, entry in entries:
            task_ngrams = _text_ngrams(prompt, ngram_size)
            best_score = 0.0
            best_ref: Path | None = None
            for ref_path, ref_ngrams in instruction_refs:
                score = _jaccard(task_ngrams, ref_ngrams)
                if score > best_score:
                    best_score = score
                    best_ref = ref_path
            if best_score >= jaccard_threshold:
                removed += 1
                ref_label = str(best_ref) if best_ref is not None else "unknown"
                self.console.print(
                    f"[yellow]removed by n-gram dedup[/yellow] {label} "
                    f"(score={best_score:.3f}, ref={ref_label})"
                )
                continue
            kept.append(entry)

        self.console.print(
            f"[cyan]dedup summary[/cyan] kept={len(kept)} removed={removed} "
            f"threshold={jaccard_threshold:.2f} ngram={ngram_size}"
        )
        return kept

    def _load_exported_tasks(self, output_path: Path) -> list[tuple[str, str]]:
        exported_tasks: list[tuple[str, str]] = []
        for task_toml_path in sorted(output_path.rglob("task.toml")):
            task_dir = task_toml_path.parent
            instruction_path = task_dir / "instruction.md"
            if not instruction_path.exists():
                continue
            try:
                prompt = instruction_path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                prompt = instruction_path.read_text(encoding="utf-8", errors="replace")
            exported_tasks.append((task_dir.relative_to(output_path).as_posix(), prompt))
        return exported_tasks

    def _remove_empty_parent_dirs(self, current_dir: Path, *, stop_dir: Path) -> None:
        while current_dir != stop_dir and current_dir.exists():
            try:
                next(current_dir.iterdir())
            except StopIteration:
                current_dir.rmdir()
                current_dir = current_dir.parent
                continue
            break

    def _load_instruction_ngrams(
        self,
        instruction_dir: Path,
        ngram_size: int,
    ) -> list[tuple[Path, set[str]]]:
        refs: list[tuple[Path, set[str]]] = []
        for path in sorted(instruction_dir.rglob("instruction.md")):
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                text = path.read_text(encoding="utf-8", errors="replace")
            refs.append((path, _text_ngrams(text, ngram_size)))
        return refs


def load_seed_records(path: Path) -> list[SeedRecord]:
    if path.is_dir():
        records: list[SeedRecord] = []
        jsonl_paths = sorted(candidate for candidate in path.glob("*.jsonl") if candidate.is_file())
        if not jsonl_paths:
            raise ValueError(f"no seed JSONL files found in directory: {path}")
        for jsonl_path in jsonl_paths:
            records.extend(_load_seed_records_from_jsonl(jsonl_path))
        return records
    return _load_seed_records_from_jsonl(path)


def _load_seed_records_from_jsonl(path: Path) -> list[SeedRecord]:
    records: list[SeedRecord] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            payload = json.loads(stripped)
            try:
                records.append(SeedRecord.model_validate(payload))
            except Exception as exc:  # noqa: BLE001
                raise ValueError(
                    f"invalid seed record in {path} at line {line_number}: {exc}"
                ) from exc
    return records


def _resolve_seed_domain(task: DatasetTask, domain_candidates: list[str]) -> str:
    raw_domain = (task.info or {}).get("domain")
    if not isinstance(raw_domain, str) or not raw_domain.strip():
        raise ValueError("seed-based task must return a non-empty info.domain")
    domain = raw_domain.strip()
    if domain not in domain_candidates:
        allowed = ", ".join(domain_candidates)
        raise ValueError(
            f"seed-based task returned invalid info.domain {domain!r}; allowed domains: {allowed}"
        )
    return domain


def _validate_task_plan(
    plan: TaskPlan,
    request: GenerationRequest,
    domains: list[DomainSpec],
) -> None:
    allowed_domains = {domain.name for domain in domains}
    if plan.domain not in allowed_domains:
        allowed = ", ".join(sorted(allowed_domains))
        raise ValueError(
            f"planner returned invalid domain {plan.domain!r}; allowed domains: {allowed}"
        )
    if plan.difficulty != request.difficulty:
        raise ValueError(
            f"planner returned difficulty {plan.difficulty!r}; expected {request.difficulty!r}"
        )
    plan_text = " ".join(
        [
            plan.objective,
            plan.initial_state,
            *plan.workflow,
            *plan.deliverables,
            *plan.verification,
            *plan.constraints,
        ]
    )
    if "/workspace" in plan_text or "workspace/" in plan_text:
        raise ValueError("planner plan contains package-authoring workspace paths; use /app paths")


def _normalize_prompt(prompt: str) -> str:
    return " ".join(prompt.lower().split())


def _is_skill_generation_mode(mode: GenerationMode) -> bool:
    return mode in {GenerationMode.SKILL_BASED, GenerationMode.AGENT_SKILL_BASED}


def _normalize_dedup_text(text: str) -> str:
    lowered = text.lower()
    lowered = re.sub(r"`[^`]+`", " CODE ", lowered)
    lowered = re.sub(r"/[a-z0-9._/-]+", " PATH ", lowered)
    lowered = re.sub(r"\b\d+\b", " NUM ", lowered)
    lowered = re.sub(r"[^a-z0-9\s]+", " ", lowered)
    return " ".join(lowered.split())


def _text_ngrams(text: str, ngram_size: int) -> set[str]:
    normalized = _normalize_dedup_text(text)
    tokens = normalized.split()
    if not tokens:
        return set()
    if len(tokens) < ngram_size:
        return {" ".join(tokens)}
    return {" ".join(tokens[index : index + ngram_size]) for index in range(len(tokens) - ngram_size + 1)}


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 1.0
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)
