import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { vi } from "vitest";

import type { PipelineArtifactDetail } from "../../../api/client";
import BehaviorRolloutViewer from "../../../components/artifacts/BehaviorRolloutViewer";

const digest = (value: string) => `sha256:${value.repeat(64)}`;
const roles = [
  ["rollout_hdf5", "payload/rollout.hdf5", "application/x-hdf5", 20],
  ["bddl_transitions", "payload/events.json", "application/json", 200],
  ["scene_metadata", "payload/scene.json", "application/json", 200],
  ["rgb_head", "payload/head.mp4", "video/mp4", 100],
  ["rgb_left_wrist", "payload/left.mp4", "video/mp4", 100],
  ["rgb_right_wrist", "payload/right.mp4", "video/mp4", 100],
  ["rgb_composite", "payload/composite.mp4", "video/mp4", 100],
] as const;

function fixture(eventSize = 200): {
  artifact: PipelineArtifactDetail;
  semantic: unknown;
  urls: Record<string, string>;
} {
  const descriptors = roles.map(([role, relative_path, media_type, size], index) => ({
    role,
    relative_path,
    media_type,
    size_bytes: role === "bddl_transitions" ? eventSize : size,
    sha256: digest(String(index + 1)),
    ...(media_type === "video/mp4" ? {
      frame_count: 60, fps: 30, codec: "h264", pixel_format: "yuv420p", width: 224, height: 224,
    } : {}),
  }));
  const files = [
    {
      file_index: 0,
      relative_path: "artifact.json",
      role: "semantic_document",
      media_type: "application/json",
      size_bytes: 1000,
      sha256: digest("a"),
      download_path: "/api/v1/pipeline-artifacts/artifact-1/files/0",
    },
    ...descriptors.map((item, index) => ({
      file_index: index + 1,
      relative_path: item.relative_path,
      role: "payload",
      media_type: item.media_type,
      size_bytes: item.size_bytes,
      sha256: item.sha256,
      download_path: `/api/v1/pipeline-artifacts/artifact-1/files/${index + 1}`,
    })),
  ];
  const artifact: PipelineArtifactDetail = {
    id: "artifact-1", name: "rollout", artifact_type: "behavior_rollout_bundle.v1",
    content_sha256: digest("b"), manifest_sha256: digest("c"), stored_size_bytes: 1020,
    file_count: files.length, safety_state: "verified_internal", visibility: "team",
    share_status: "pending_scan", access_class: "team_runtime", download_path: "/download", detail_path: "/detail",
    pipeline_run_id: "run-1", pipeline_stage_run_id: "stage-1",
    execution_attempt_id: "attempt-1", producer_kind: "container",
    created_at: "2026-08-12T00:00:00Z", lineage_artifact_ids: ["input-1"],
    lineage_digests: [digest("d")], files,
  };
  return {
    artifact,
    semantic: {
      schema_version: "behavior_rollout_bundle.v1",
      payload: {
        task_name: "placing_can", demo_stem: "episode_00000001",
        domain_outcome: "rollout_success", success: true, step_count: 60,
        recording_fps: 30, required_file_descriptors: descriptors, optional_audit_files: [],
      },
      files: [], provenance: {},
    },
    urls: Object.fromEntries(files.map((item) => [
      item.relative_path,
      `/api/v1/pipeline-artifacts/artifact-1/files/${item.file_index}`,
    ])),
  };
}

function jsonResponse(value: unknown): Response {
  const body = JSON.stringify(value);
  return new Response(body, {
    status: 200,
    headers: { "Content-Type": "application/json", "Content-Length": String(body.length) },
  });
}

afterEach(() => vi.restoreAllMocks());

test("renders four synchronized videos, bounded searchable events, scene tables, and HDF5 download", async () => {
  const caseData = fixture();
  const eventDocument = {
    transitions: Array.from({ length: 150 }, (_, step_idx) => ({
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
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const url = String(input);
    if (url === caseData.urls["artifact.json"]) return jsonResponse(caseData.semantic);
    if (url === caseData.urls["payload/events.json"]) return jsonResponse(eventDocument);
    if (url === caseData.urls["payload/scene.json"]) return jsonResponse(scene);
    throw new Error(`unexpected ${url}`);
  });
  const view = render(<BehaviorRolloutViewer artifact={caseData.artifact} />);

  expect(await screen.findByRole("heading", { name: "Behavior rollout" })).toBeInTheDocument();
  const videos = Array.from(view.container.querySelectorAll("video"));
  expect(videos).toHaveLength(4);
  for (const video of videos) {
    expect(video).toHaveAttribute("controls");
    expect(video).toHaveAttribute("preload", "metadata");
    expect(video.autoplay).toBe(false);
  }
  await screen.findByText("150 matching events");
  expect(screen.getAllByRole("listitem")).toHaveLength(100);
  fireEvent.change(screen.getByLabelText("Search events"), { target: { value: "predicate-149" } });
  expect(screen.getAllByRole("listitem")).toHaveLength(1);
  fireEvent.click(screen.getByRole("button", { name: /step 149/ }));
  expect(videos[0].currentTime).toBeCloseTo(149 / 30);
  expect(videos[3].currentTime).toBeCloseTo(149 / 30);

  videos[0].currentTime = 1;
  videos[1].currentTime = 0.8;
  videos[2].currentTime = 0.95;
  fireEvent.timeUpdate(videos[0]);
  expect(videos[1].currentTime).toBe(1);
  expect(videos[2].currentTime).toBe(0.95);
  expect(await screen.findByText("Robot:")).toBeInTheDocument();
  expect(screen.getAllByText("can")).toHaveLength(2);
  expect(screen.getByRole("link", { name: "Download rollout HDF5" })).toHaveAttribute(
    "href",
    caseData.urls["payload/rollout.hdf5"],
  );
});

test("does not parse event JSON at 16 MiB plus one and keeps a download fallback", async () => {
  const caseData = fixture(16 * 1024 * 1024 + 1);
  const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const url = String(input);
    if (url === caseData.urls["artifact.json"]) return jsonResponse(caseData.semantic);
    if (url === caseData.urls["payload/scene.json"]) return jsonResponse({
      schema_version: "behavior.rollout-scene-projection.v1",
      robot_scene_name: "robot", state_objects: [], inst_to_name: [],
    });
    throw new Error(`large event body must not be fetched: ${url}`);
  });
  render(<BehaviorRolloutViewer artifact={caseData.artifact} />);
  expect(await screen.findByText(/larger than 16 MiB/)).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "Download events" })).toHaveAttribute(
    "href",
    caseData.urls["payload/events.json"],
  );
  expect(fetchMock.mock.calls.map(([input]) => String(input))).not.toContain(
    caseData.urls["payload/events.json"],
  );
});

test("allows an event descriptor of exactly 16 MiB", async () => {
  const caseData = fixture(16 * 1024 * 1024);
  const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const url = String(input);
    if (url === caseData.urls["artifact.json"]) return jsonResponse(caseData.semantic);
    if (url === caseData.urls["payload/events.json"]) {
      return jsonResponse({ transitions: [], grasp_history: [] });
    }
    if (url === caseData.urls["payload/scene.json"]) return jsonResponse({
      schema_version: "behavior.rollout-scene-projection.v1",
      robot_scene_name: "robot",
      state_objects: [{ ordinal: 0, scene_name: "robot", joint_position_count: 28 }],
      inst_to_name: [{ scope_name: "robot", scene_name: "robot" }],
    });
    throw new Error(`unexpected ${url}`);
  });
  render(<BehaviorRolloutViewer artifact={caseData.artifact} />);
  expect(await screen.findByText("0 matching events")).toBeInTheDocument();
  expect(fetchMock.mock.calls.map(([input]) => String(input))).toContain(
    caseData.urls["payload/events.json"],
  );
});

test("aborts pending JSON reads on unmount and ignores their late completion", async () => {
  const caseData = fixture();
  let observedSignal: AbortSignal | undefined;
  vi.spyOn(globalThis, "fetch").mockImplementation((_input, init) => {
    observedSignal = init?.signal ?? undefined;
    return new Promise<Response>(() => undefined);
  });
  const view = render(<BehaviorRolloutViewer artifact={caseData.artifact} />);
  await waitFor(() => expect(observedSignal).toBeDefined());
  view.unmount();
  expect(observedSignal?.aborted).toBe(true);
});
