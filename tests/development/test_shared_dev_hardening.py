import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


class SharedDevHardeningTest(unittest.TestCase):
    def test_compose_host_ports_default_to_loopback_bindings(self):
        compose = _read("docker-compose.dev.yml")

        for binding in (
            "${ADP_API_HOST_BIND:-127.0.0.1}:${ADP_API_PORT:-8000}:8000",
            "${ADP_POSTGRES_HOST_BIND:-127.0.0.1}:${ADP_POSTGRES_PORT:-5432}:5432",
            "${ADP_REDIS_HOST_BIND:-127.0.0.1}:${ADP_REDIS_PORT:-6379}:6379",
            "${ADP_MINIO_HOST_BIND:-127.0.0.1}:${ADP_MINIO_PORT:-9000}:9000",
            "${ADP_MINIO_CONSOLE_HOST_BIND:-127.0.0.1}:${ADP_MINIO_CONSOLE_PORT:-9001}:9001",
        ):
            self.assertIn(binding, compose)

        self.assertNotIn('"8000:8000"', compose)
        self.assertNotIn('"5432:5432"', compose)
        self.assertNotIn('"6379:6379"', compose)
        self.assertNotIn('"9000:9000"', compose)
        self.assertNotIn('"9001:9001"', compose)

    def test_docker_runtime_versions_are_controlled_by_constraints(self):
        constraints = _read("constraints/dev-runtime.txt")
        dockerfile = _read("Dockerfile.dev")
        pyproject = _read("pyproject.toml")

        self.assertIn("harbor==0.9.0", constraints)
        self.assertIn("fastapi>=0.136,<0.137", constraints)
        self.assertIn("httpx>=0.28,<0.29", constraints)
        self.assertIn("httpx2>=2.3,<3.0", constraints)
        self.assertIn("starlette>=1.2,<1.3", constraints)
        self.assertIn("PIP_CONSTRAINT=constraints/dev-runtime.txt", dockerfile)
        self.assertIn("COPY constraints ./constraints", dockerfile)
        self.assertNotIn('ARG HARBOR_VERSION=', dockerfile)
        self.assertIn('"harbor==0.9.0"', pyproject)

    def test_ci_builds_docker_runtime_and_validates_compose_config(self):
        ci = _read(".github/workflows/ci.yml")

        self.assertIn("docker build -f Dockerfile.dev -t agentic-data-shared dev:ci .", ci)
        self.assertIn("docker compose -f docker-compose.dev.yml config --quiet", ci)


def _read(path: str) -> str:
    return (REPO_ROOT / path).read_text(encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
