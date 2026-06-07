"""LLM-judge rubric template for GAIA.

Uses plain `<<REFERENCE_ANSWER>>` / `<<CANDIDATE_ANSWER>>` markers
instead of `str.format` placeholders so the rubric body (which
contains literal `{"score": ...}` JSON) doesn't collide with the
verifier's substitution pass. The adapter replaces
`<<REFERENCE_ANSWER>>` at convert-time and the llm-judge verifier
replaces `<<CANDIDATE_ANSWER>>` at verify-time.
"""

GAIA_RUBRIC_TEMPLATE = """
You are grading an open-ended response.

REFERENCE ANSWER: <<REFERENCE_ANSWER>>

CANDIDATE ANSWER: <<CANDIDATE_ANSWER>>

Award score=1.0 if the candidate answer matches the reference answer
in substance (ignore wording, case, and trailing punctuation). For
numeric answers, allow 0.5% tolerance. For lists, accept any order.
Award 0.0 otherwise. Return JSON: {"score": float, "reasoning": str}.
""".strip()
