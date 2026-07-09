# Harbor conformance tests (optional)

Loom embeds pinned Harbor `Terminus2` in the worker image (`deploy/Dockerfile.worker`).
Conformance against Harbor `v0.18.0@527d50d` is validated by running the real agent in
worker integration tests when Harbor is installed (`pytest.importorskip("harbor")`).

To exercise Harbor-backed paths locally:

```bash
pip install -e /path/to/harbor  # pinned at 527d50d
uv run pytest tests/unit/terminus2/test_harbor_environment.py -q
```

Harbor is a **worker-image** dependency only; it is not required for the main Loom package.
