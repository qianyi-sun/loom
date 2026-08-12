import AxeBuilder from "@axe-core/playwright";

import { expect, test } from "./fixtures/guardedTest";

const runId = "00000000-0000-0000-0000-000000000001";
const stageId = "00000000-0000-0000-0000-000000000002";
const artifactId = "00000000-0000-0000-0000-000000000003";
const digest = (value: string) => `sha256:${value.repeat(64)}`;
const descriptors = [
  { role: "rollout_hdf5", relative_path: "payload/rollout.hdf5", media_type: "application/x-hdf5", size_bytes: 20, sha256: digest("1") },
  { role: "bddl_transitions", relative_path: "payload/events.json", media_type: "application/json", size_bytes: 20_000, sha256: digest("2") },
  { role: "scene_metadata", relative_path: "payload/scene.json", media_type: "application/json", size_bytes: 500, sha256: digest("3") },
  { role: "rgb_head", relative_path: "payload/head.mp4", media_type: "video/mp4", size_bytes: 100, sha256: digest("4") },
  { role: "rgb_left_wrist", relative_path: "payload/left.mp4", media_type: "video/mp4", size_bytes: 100, sha256: digest("5") },
  { role: "rgb_right_wrist", relative_path: "payload/right.mp4", media_type: "video/mp4", size_bytes: 100, sha256: digest("6") },
  { role: "rgb_composite", relative_path: "payload/composite.mp4", media_type: "video/mp4", size_bytes: 100, sha256: digest("7") },
];
const files = [
  { file_index: 0, relative_path: "artifact.json", role: "semantic_document", media_type: "application/json", size_bytes: 1000, sha256: digest("a"), download_path: `/api/v1/pipeline-artifacts/${artifactId}/files/0` },
  ...descriptors.map((item, index) => ({
    file_index: index + 1, relative_path: item.relative_path, role: "payload",
    media_type: item.media_type, size_bytes: item.size_bytes, sha256: item.sha256,
    download_path: `/api/v1/pipeline-artifacts/${artifactId}/files/${index + 1}`,
  })),
];
const artifact = {
  id: artifactId, name: "rollout", artifact_type: "behavior_rollout_bundle.v1",
  content_sha256: digest("b"), manifest_sha256: digest("c"), stored_size_bytes: 21_000,
  file_count: files.length, safety_state: "verified_internal", visibility: "team",
  share_status: "pending_scan", download_path: `/api/v1/pipeline-artifacts/${artifactId}/download`,
  detail_path: `/pipelines/${runId}/stages/${stageId}/artifacts/${artifactId}`,
  pipeline_run_id: runId, pipeline_stage_run_id: stageId,
  execution_attempt_id: "00000000-0000-0000-0000-000000000004",
  producer_kind: "container", created_at: "2026-08-12T00:00:00Z",
  lineage_artifact_ids: ["00000000-0000-0000-0000-000000000005"],
  lineage_digests: [digest("d")], files,
};
const semantic = {
  schema_version: "behavior_rollout_bundle.v1",
  payload: {
    task_name: "placing_can", demo_stem: "episode_00000001",
    domain_outcome: "rollout_success", success: true, step_count: 140,
    recording_fps: 30, required_file_descriptors: descriptors, optional_audit_files: [],
  },
  files: [], provenance: {},
};
const events = {
  transitions: Array.from({ length: 140 }, (_, step_idx) => ({
    step_idx, predicate_id: `predicate-${step_idx}`, predicate_name: "Inside",
    old_value: false, new_value: true, obj_a: `object-${step_idx}`,
  })),
  grasp_history: [],
};
const scene = {
  schema_version: "behavior.rollout-scene-projection.v1",
  robot_scene_name: "robot",
  state_objects: [
    { ordinal: 0, scene_name: "robot", joint_position_count: 28 },
    { ordinal: 1, scene_name: "can", joint_position_count: 0 },
  ],
  inst_to_name: [{ scope_name: "object", scene_name: "can" }],
};

test("Behavior rollout Artifact is navigable, synchronized, bounded, searchable, and accessible", async ({
  apiHarness,
  browserHarness,
  page,
}) => {
  await apiHarness.install({
    role: "user",
    overrides: [
      {
        name: "typed Artifact detail",
        method: "GET",
        path: `/api/v1/pipeline-runs/${runId}/stages/${stageId}/artifacts/${artifactId}`,
        response: { kind: "json", status: 200, body: artifact },
      },
      {
        name: "rollout semantic document",
        method: "GET",
        path: `/api/v1/pipeline-artifacts/${artifactId}/files/0`,
        response: { kind: "json", status: 200, body: semantic },
      },
      {
        name: "rollout transition events",
        method: "GET",
        path: `/api/v1/pipeline-artifacts/${artifactId}/files/2`,
        response: { kind: "json", status: 200, body: events },
      },
      {
        name: "rollout scene projection",
        method: "GET",
        path: `/api/v1/pipeline-artifacts/${artifactId}/files/3`,
        response: { kind: "json", status: 200, body: scene },
      },
    ],
  });
  await page.route(new RegExp(`/api/v1/pipeline-artifacts/${artifactId}/files/[4-7]$`, "u"), async (route) => {
    await route.fulfill({ status: 200, contentType: "video/mp4", body: "not-decoded-by-this-contract-test" });
  });
  await page.goto(`${browserHarness.baseURL}/pipelines/${runId}/stages/${stageId}/artifacts/${artifactId}`);

  await expect(page.getByRole("heading", { name: "Behavior rollout" })).toBeVisible();
  const videos = page.locator("video");
  await expect(videos).toHaveCount(4);
  for (let index = 0; index < 4; index += 1) {
    await expect(videos.nth(index)).toHaveAttribute("controls", "");
    await expect(videos.nth(index)).toHaveAttribute("preload", "metadata");
    await expect(videos.nth(index)).not.toHaveAttribute("autoplay", /.*/u);
  }
  await expect(page.getByText("140 matching events")).toBeVisible();
  const eventList = page.getByRole("list", { name: "Virtualized rollout events" });
  await expect(eventList.getByRole("listitem")).toHaveCount(100);
  await page.getByLabel("Search events").fill("predicate-139");
  await expect(eventList.getByRole("listitem")).toHaveCount(1);
  await eventList.getByRole("button", { name: /step 139/ }).click();
  expect(await videos.nth(0).evaluate((video: HTMLVideoElement) => video.currentTime)).toBeCloseTo(139 / 30);
  await expect(page.getByRole("table", { name: "Scene objects" })).toContainText("can");
  await expect(page.getByRole("link", { name: "Download rollout HDF5" })).toHaveAttribute(
    "href",
    `${browserHarness.routePrefix}/api/v1/pipeline-artifacts/${artifactId}/files/1`,
  );
  await expect(page.getByRole("heading", { name: "Provenance and lineage" })).toBeVisible();
  const axe = await new AxeBuilder({ page }).analyze();
  expect(axe.violations).toEqual([]);
});
