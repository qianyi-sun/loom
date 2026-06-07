"""LLM-judge rubric template for GAIA.

The adapter formats `{reference_answer}` at convert-time and leaves
`{candidate_answer}` as a literal placeholder — the llm-judge verifier
substitutes the candidate answer at verify-time.
"""

GAIA_RUBRIC_TEMPLATE = """
You are grading an open-ended response.

REFERENCE ANSWER: {reference_answer}

CANDIDATE ANSWER: {candidate_answer}

Award score=1.0 if the candidate answer matches the reference answer
in substance (ignore wording, case, and trailing punctuation). For
numeric answers, allow 0.5% tolerance. For lists, accept any order.
Award 0.0 otherwise. Return JSON: {{"score": float, "reasoning": str}}.
""".strip()
