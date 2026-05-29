import unittest
from pathlib import Path


class DevImageContractTest(unittest.TestCase):
    def test_dev_image_installs_docker_compose_plugin_for_harbor(self):
        dockerfile = Path(__file__).resolve().parents[2] / "Dockerfile.dev"
        contents = dockerfile.read_text(encoding="utf-8")

        self.assertIn("ARG DOCKER_COMPOSE_VERSION=", contents)
        self.assertIn("DOCKER_COMPOSE_SHA256_X86_64", contents)
        self.assertIn("DOCKER_COMPOSE_SHA256_AARCH64", contents)
        self.assertIn("/usr/local/lib/docker/cli-plugins/docker-compose", contents)
        self.assertIn("docker compose version", contents)


if __name__ == "__main__":
    unittest.main()
