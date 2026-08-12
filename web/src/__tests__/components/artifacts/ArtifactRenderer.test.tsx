import { render, screen } from "@testing-library/react";

import type { PipelineArtifactDetail } from "../../../api/client";
import ArtifactRenderer, {
  ARTIFACT_RENDERERS,
} from "../../../components/artifacts/ArtifactRenderer";

const generic: PipelineArtifactDetail = {
  id: "artifact-1",
  name: "opaque-result",
  artifact_type: "custom.opaque.v1",
  content_sha256: `sha256:${"1".repeat(64)}`,
  manifest_sha256: `sha256:${"2".repeat(64)}`,
  stored_size_bytes: 3,
  file_count: 1,
  safety_state: "verified_internal",
  visibility: "team",
  share_status: "pending_scan",
  download_path: "/api/v1/pipeline-artifacts/artifact-1/download",
  detail_path: "/pipelines/run-1/stages/stage-1/artifacts/artifact-1",
  pipeline_run_id: "run-1",
  pipeline_stage_run_id: "stage-1",
  execution_attempt_id: "attempt-1",
  producer_kind: "container",
  created_at: "2026-08-12T00:00:00Z",
  lineage_artifact_ids: [],
  lineage_digests: [],
  files: [{
    file_index: 0,
    relative_path: "payload/result.bin",
    role: "payload",
    media_type: "application/octet-stream",
    size_bytes: 3,
    sha256: `sha256:${"3".repeat(64)}`,
    download_path: "/api/v1/pipeline-artifacts/artifact-1/files/0",
  }],
};

test("uses an exact artifact_type registry and generic fallback", () => {
  expect(Object.keys(ARTIFACT_RENDERERS)).toEqual(["behavior_rollout_bundle.v1"]);
  render(<ArtifactRenderer artifact={generic} />);
  expect(screen.getByRole("heading", { name: "Artifact files" })).toBeInTheDocument();
  expect(screen.getByText(/payload\/result.bin/)).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "Download" })).toHaveAttribute(
    "href",
    generic.files[0].download_path,
  );
});
test("does not sniff a rollout-looking filename or MIME type", () => {
  render(<ArtifactRenderer artifact={{
    ...generic,
    files: [{
      ...generic.files[0],
      relative_path: "payload/rgb_composite.mp4",
      media_type: "video/mp4",
    }],
  }} />);
  expect(screen.getByRole("heading", { name: "Artifact files" })).toBeInTheDocument();
  expect(screen.queryByRole("heading", { name: "Behavior rollout" })).not.toBeInTheDocument();
});
