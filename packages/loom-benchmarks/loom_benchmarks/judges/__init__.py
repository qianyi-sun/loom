"""LLM-judge prompt templates used by benchmarks whose verifier is
`llm-judge`. Each judge module exports a `_RUBRIC_TEMPLATE` string that
the adapter formats with the per-instance reference answer."""

from loom_benchmarks.judges.gaia import GAIA_RUBRIC_TEMPLATE

__all__ = ["GAIA_RUBRIC_TEMPLATE"]
