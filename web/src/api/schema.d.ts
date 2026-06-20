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
      parameters: {
        query?: {
          cursor?: string;
          limit?: number;
          include_empty?: boolean;
        };
      };
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
  "/api/v1/batches": {
    get: {
      parameters: {
        query?: {
          team_id?: string;
          state?: string;
          cursor?: string;
          limit?: number;
        };
      };
      responses: {
        200: {
          content: {
            "application/json": components["schemas"]["BatchList"];
          };
        };
      };
    };
    post: {
      requestBody: {
        content: {
          "application/json": {
            name: string;
            description?: string;
            backend?: string;
            task_filter: Record<string, unknown>;
            trial_config: Record<string, unknown>;
            n_per_task?: number;
            combinations?: components["schemas"]["Combination"][];
          };
        };
      };
      responses: {
        201: {
          content: {
            "application/json": {
              batch_id: string;
              expected_trial_count: number;
              state: string;
              created_at: string;
            };
          };
        };
      };
    };
  };
  "/api/v1/backends": {
    get: {
      responses: {
        200: {
          content: {
            "application/json": {
              items: {
                name: string;
                description: string;
                available: boolean;
              }[];
            };
          };
        };
      };
    };
  };
  "/api/v1/local-servers": {
    get: {
      responses: {
        200: {
          content: {
            "application/json": {
              items: {
                name: string;
                base_url: string;
                kind: string | null;
                description: string | null;
              }[];
            };
          };
        };
      };
    };
  };
  "/api/v1/batches/{id}": {
    get: {
      parameters: { path: { id: string } };
      responses: {
        200: {
          content: {
            "application/json": components["schemas"]["BatchDetail"];
          };
        };
      };
    };
  };
  "/api/v1/batches/{id}/cancel": {
    post: {
      parameters: { path: { id: string } };
      responses: {
        200: {
          content: {
            "application/json": { batch_id: string; state: string };
          };
        };
      };
    };
  };
  "/api/v1/batches/{id}/rerun-failed": {
    post: {
      parameters: { path: { id: string } };
      responses: {
        201: {
          content: {
            "application/json": {
              batch_id: string;
              rerun_of_batch_id: string;
              expected_trial_count: number;
              state: string;
              created_at: string;
              rerun_target_count: number;
            };
          };
        };
      };
    };
  };
  "/api/v1/rate-cards": {
    get: {
      responses: {
        200: { content: { "application/json": { items: unknown[] } } };
      };
    };
    post: {
      requestBody: {
        content: { "application/json": Record<string, unknown> };
      };
      responses: { 201: { content: { "application/json": unknown } } };
    };
  };
  "/api/v1/usage": {
    get: {
      parameters: {
        query: {
          team_id?: string;
          start: string;
          end: string;
          group_by?: string;
        };
      };
      responses: {
        200: {
          content: { "application/json": components["schemas"]["Usage"] };
        };
      };
    };
  };
  "/api/v1/teams/{team_id}": {
    get: {
      parameters: { path: { team_id: string } };
      responses: {
        200: { content: { "application/json": components["schemas"]["Team"] } };
      };
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
      model: components["schemas"]["ModelSpec"] | null;
    };
    ModelSpec: {
      provider: string;
      name: string;
      source?: string;
      local_server?: string | null;
      hf_execution?: string;
      tier?: string | null;
      region?: string | null;
      max_input_tokens?: number | null;
      max_output_tokens?: number | null;
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
      artifacts: {
        step_name?: string;
        key: string;
        size: number;
        download_url: string;
      }[];
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
      task_count: number;
      raw_task_count?: number;
      valid_task_config_count?: number;
      invalid_task_config_count?: number;
      source_schemes?: string[];
      adapter_status?: string;
      manifest_status?: string;
      materializer_status?: string;
      smoke_status?: string;
      readiness_state?: string;
      readiness_label?: string;
      readiness_message?: string | null;
      selectable?: boolean;
      blocker_reason?: string | null;
      series?: string | null;
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
    Batch: {
      id: string;
      team_id: string;
      name: string;
      description: string | null;
      task_filter: Record<string, unknown>;
      trial_config: Record<string, unknown>;
      backend: string;
      combinations: components["schemas"]["Combination"][];
      state: string;
      result_status: string | null;
      failure_reason: string | null;
      failure_message: string | null;
      fanout_errors: Record<string, unknown>[];
      rerun_of_batch_id: string | null;
      rerun_targets: Record<string, unknown>[];
      created_at: string;
      finished_at: string | null;
      created_by_token_prefix: string;
      expected_trial_count: number;
    };
    Combination: {
      label?: string;
      agent_name: string;
      agent_model: { provider: string; name: string } | null;
      n_per_task: number;
    };
    BatchList: {
      items: components["schemas"]["Batch"][];
      next_cursor: string | null;
    };
    BatchDetail: components["schemas"]["Batch"] & {
      trial_summary: Record<string, number>;
      aggregate_reward: number | null;
      total_cost_usd: number;
      rerun_batches: {
        id: string;
        name: string;
        state: string;
        result_status: string | null;
        expected_trial_count: number;
        created_at: string;
        finished_at: string | null;
      }[];
      rerunnable_failed_count: number;
      effective_trial_summary: Record<string, number>;
      effective_result_status: string | null;
      effective_aggregate_reward: number | null;
      effective_total_cost_usd: number;
    };
    UsageBucket: {
      start_at: string;
      end_at: string | null;
      trial_count: number;
      trials_currently_succeeded: number;
      trials_currently_failed: number;
      succeeded_count: number;
      failed_count: number;
      total_cost_usd: number;
      llm_input_tokens: number;
      llm_output_tokens: number;
    };
    Usage: {
      buckets: components["schemas"]["UsageBucket"][];
      degraded: boolean;
    };
    TeamMember: {
      token_hash_prefix: string;
      type: string;
      scopes: string[];
      issued_at: string;
      expires_at: string | null;
      revoked_at: string | null;
      last_seen_at: string | null;
    };
    TeamQuota: {
      fair_share_weight: number;
      max_attempts: number;
      in_flight_count: number;
      license_allowlist: string[];
    };
    Team: {
      id: string;
      name: string;
      created_at: string;
      quota: components["schemas"]["TeamQuota"] | null;
      members: components["schemas"]["TeamMember"][];
    };
  };
}
