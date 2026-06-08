// Hand-stubbed types for the loom_service API surface (matches the
// shape that Plans 17-20 actually return). When `loom_service` is
// reachable at build time, regenerate via `npm run gen-api` to
// replace this with the OpenAPI-derived version.

export interface paths {
  "/api/v1/health": {
    get: {
      responses: {
        200: { content: { "application/json": { status: string } } };
      };
    };
  };
  "/api/v1/trials": {
    get: {
      parameters: {
        query?: {
          team_id?: string;
          task_id?: string;
          state?: string;
          cursor?: string;
          limit?: number;
        };
      };
      responses: {
        200: {
          content: { "application/json": components["schemas"]["TrialList"] };
        };
      };
    };
  };
  "/api/v1/trials/{trial_id}": {
    get: {
      parameters: { path: { trial_id: string } };
      responses: {
        200: {
          content: {
            "application/json": components["schemas"]["TrialDetail"];
          };
        };
      };
    };
  };
  "/api/v1/trials/{trial_id}/trajectory": {
    get: {
      parameters: {
        path: { trial_id: string };
        query?: { cursor?: number; limit?: number };
      };
      responses: {
        200: {
          content: {
            "application/json": components["schemas"]["TrajectoryPage"];
          };
        };
      };
    };
  };
  "/api/v1/tasks": {
    get: {
      parameters: {
        query?: {
          benchmark_id?: string;
          license?: string;
          cursor?: string;
          limit?: number;
        };
      };
      responses: {
        200: {
          content: { "application/json": components["schemas"]["TaskList"] };
        };
      };
    };
  };
  "/api/v1/benchmarks": {
    get: {
      parameters: { query?: { cursor?: string; limit?: number } };
      responses: {
        200: {
          content: {
            "application/json": components["schemas"]["BenchmarkList"];
          };
        };
      };
    };
  };
  "/api/v1/tokens": {
    get: {
      responses: {
        200: {
          content: { "application/json": components["schemas"]["TokenList"] };
        };
      };
    };
    post: {
      requestBody: {
        content: {
          "application/json": {
            type: string;
            scopes: string[];
            expires_in_days: number;
            team_id?: string;
          };
        };
      };
      responses: {
        201: {
          content: {
            "application/json": {
              token: string;
              token_hash_prefix: string;
              expires_at: string;
            };
          };
        };
      };
    };
  };
  "/api/v1/tokens/{prefix}": {
    delete: {
      parameters: { path: { prefix: string } };
      responses: { 204: { content: never } };
    };
  };
}

export interface components {
  schemas: {
    Trial: {
      id: string;
      task_id: string;
      team_id: string;
      state: string;
      failure_reason: string | null;
      submitted_at: string;
      started_at: string | null;
      finished_at: string | null;
      attempt_count: number;
      aggregate_reward: number | null;
      cost_usd: number;
      agent_name: string | null;
      model: string | null;
    };
    TrialList: {
      items: components["schemas"]["Trial"][];
      next_cursor: string | null;
    };
    TrialDetail: components["schemas"]["Trial"] & {
      atif_url: string;
      trajectory_url: string;
      atif_ready: boolean;
      trajectory_ready: boolean;
      artifacts: { key: string; size: number; download_url: string }[];
    };
    TrajectoryEvent: {
      kind: string;
      trial_id?: string;
      step_id?: string | null;
      seq?: number;
      emitted_at?: string;
      [k: string]: unknown;
    };
    TrajectoryPage: {
      events: components["schemas"]["TrajectoryEvent"][];
      next_cursor: number | null;
    };
    Task: {
      id: string;
      checksum: string;
      source: string | null;
      license: string | null;
      benchmark_id: string | null;
      registered_at: string;
    };
    TaskList: {
      items: components["schemas"]["Task"][];
      next_cursor: string | null;
    };
    Benchmark: {
      id: string;
      display_name: string;
      upstream_kind: string;
      upstream_locator: string;
      upstream_revision: string;
      license_spdx: string;
      license_url: string;
      splits: string[];
      imported_at: string;
      imported_by: string | null;
    };
    BenchmarkList: {
      items: components["schemas"]["Benchmark"][];
      next_cursor: string | null;
    };
    Token: {
      token_hash_prefix: string;
      type: string;
      scopes: string[];
      team_id: string | null;
      issued_at: string;
      expires_at: string | null;
      revoked_at: string | null;
    };
    TokenList: { items: components["schemas"]["Token"][] };
  };
}
