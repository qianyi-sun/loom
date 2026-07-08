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
  "/api/v1/monitor/summary": {
    get: {
      parameters: {
        query?: {
          view?: "batches" | "trials";
          team_id?: string;
          q?: string;
          benchmark_id?: string;
          agent_name?: string;
          agent?: string;
          model_provider?: string;
          model_name?: string;
          model?: string;
          provider_connection_id?: string;
          provider_model_id?: string;
          batch_id?: string;
          state?: string;
        };
      };
      responses: {
        200: {
          content: {
            "application/json": components["schemas"]["MonitorSummary"];
          };
        };
      };
    };
  };
  "/api/v1/trials": {
    get: {
      parameters: {
        query?: {
          team_id?: string;
          task_id?: string;
          batch_id?: string;
          benchmark_id?: string;
          agent_name?: string;
          agent?: string;
          model_provider?: string;
          model_name?: string;
          model?: string;
          provider_connection_id?: string;
          provider_model_id?: string;
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
  "/api/v1/trials/{trial_id}/debug": {
    get: {
      parameters: { path: { trial_id: string } };
      responses: {
        200: {
          content: {
            "application/json": components["schemas"]["DebugEvidence"];
          };
        };
      };
    };
  };
  "/api/v1/trials/{trial_id}/diagnosis": {
    get: {
      parameters: { path: { trial_id: string } };
      responses: {
        200: {
          content: {
            "application/json": components["schemas"]["DiagnosisReport"];
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
            name: string;
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
              expires_at: string | null;
              item: components["schemas"]["Token"];
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
  "/api/v1/tokens/{prefix}/rotate": {
    post: {
      parameters: { path: { prefix: string } };
      responses: {
        201: {
          content: {
            "application/json": {
              token: string;
              token_hash_prefix: string;
              expires_at: string | null;
              item: components["schemas"]["Token"];
            };
          };
        };
      };
    };
  };
  "/api/v1/batches": {
    get: {
      parameters: {
        query?: {
          team_id?: string;
          q?: string;
          benchmark_id?: string;
          agent_name?: string;
          agent?: string;
          model_provider?: string;
          model_name?: string;
          model?: string;
          provider_connection_id?: string;
          provider_model_id?: string;
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
            team_id?: string;
            name?: string;
            name_suffix?: string;
            description?: string;
            backend?: string;
            task_filter: Record<string, unknown>;
            trial_config: Record<string, unknown>;
            n_per_task?: number;
            combinations?: components["schemas"]["Combination"][];
            provider_connection_id?: string | null;
            provider_model_id?: string | null;
            budget_usd?: number | null;
            budget_policy?: "none" | "soft" | "hard";
            budget_confirmed?: boolean;
          };
        };
      };
      responses: {
        201: {
          content: {
            "application/json": {
              batch_id: string;
              team_id: string;
              expected_trial_count: number;
              state: string;
              created_at: string;
              budget_usd?: number | null;
              budget_policy?: string;
              pre_run_estimated_cost_usd?: number | null;
              budget_remaining_usd?: number | null;
              budget_status?: string;
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
  "/api/v1/batches/{id}/debug": {
    get: {
      parameters: { path: { id: string } };
      responses: {
        200: {
          content: {
            "application/json": components["schemas"]["DebugEvidence"];
          };
        };
      };
    };
  };
  "/api/v1/batches/{id}/diagnosis": {
    get: {
      parameters: { path: { id: string } };
      responses: {
        200: {
          content: {
            "application/json": components["schemas"]["DiagnosisReport"];
          };
        };
      };
    };
  };
  "/api/v1/batches/{id}/rerun-plan": {
    get: {
      parameters: {
        path: { id: string };
        query?: {
          task_id?: string[];
          include_operator_approval?: boolean;
        };
      };
      responses: {
        200: {
          content: {
            "application/json": components["schemas"]["RerunPlan"];
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
              rerun_plan: components["schemas"]["RerunPlan"];
            };
          };
        };
      };
    };
  };
  "/api/v1/batches/{id}/delivery-export": {
    get: {
      parameters: { path: { id: string } };
      responses: {
        200: {
          content: {
            "application/json": components["schemas"]["DeliveryExport"];
          };
        };
      };
    };
    post: {
      parameters: { path: { id: string } };
      requestBody: {
        content: {
          "application/json": {
            supplemental_batch_ids?: string[] | null;
          };
        };
      };
      responses: {
        201: {
          content: {
            "application/json": components["schemas"]["DeliveryExport"];
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
          include_batches?: string;
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
    MonitorSummary: {
      scope: {
        view: "batches" | "trials";
        team_id: string | null;
        q: string | null;
        benchmark_id: string | null;
        agent_name: string | null;
        model_provider: string | null;
        model_name: string | null;
        provider_connection_id: string | null;
        provider_model_id: string | null;
        batch_id: string | null;
        state: string | null;
      };
      state_counts: {
        batches: {
          submitted: number;
          running: number;
          finished: number;
          cancelled: number;
        };
        trials: {
          queued: number;
          claimed: number;
          running: number;
          succeeded: number;
          failed: number;
          cancelled: number;
        };
      };
      queue: {
        queued: number;
        claimed: number;
        running: number;
        waiting: number;
        active_workers: number;
        available_backends: string[];
        has_default_backend: boolean;
        status: "blocked" | "waiting" | "running" | "idle" | string;
      };
      resources: components["schemas"]["ResourceSummary"];
    };
    ResourceSummary: {
      aggregate: components["schemas"]["ResourceAggregate"];
      pools: components["schemas"]["ResourcePool"][];
    };
    ResourceAggregate: {
      desired_slots: number;
      pending_slots: number;
      current_active_slots: number;
      max_slots: number;
      ceiling_slots: number;
      active_workers: number;
      draining_workers: number;
      total_slots: number;
      draining_slots: number;
      occupied_slots: number;
      free_slots: number;
      running_tasks: number;
      starting_tasks: number;
      queued_tasks: number;
    };
    ResourcePool: {
      pool_name: string;
      backend: string;
      cpu_arch: string;
      autoscaler_environment: string | null;
      autoscaler_actuator: string | null;
      autoscaler_enabled: boolean;
      autoscaler_idle_since_at: string | null;
      autoscaler_idle_seconds: number | null;
      desired_slots: number;
      pending_slots: number;
      current_active_slots: number;
      max_slots: number;
      ceiling_slots: number;
      active_workers: number;
      draining_workers: number;
      total_slots: number;
      draining_slots: number;
      occupied_slots: number;
      free_slots: number;
      running_tasks: number;
      starting_tasks: number;
      queued_tasks: number;
      last_autoscaler_decision: string | null;
      last_autoscaler_reason: string | null;
      decision_reason: string | null;
      last_autoscaler_blocked_reason: string | null;
      blocked_reason: string | null;
      last_autoscaler_error: string | null;
    };
    UsageCostStatus:
      | "no_usage"
      | "estimated"
      | "not_applicable"
      | "price_unknown"
      | "failed_upstream"
      | "mixed"
      | string;
    UsagePricingMode:
      | "priced"
      | "tokens-only"
      | "price-unknown"
      | "failed-upstream"
      | string;
    UsageReportingStatus:
      | "no_usage"
      | "complete"
      | "partial"
      | "missing"
      | string;
    UsageEstimateConfidence:
      | "none"
      | "high"
      | "partial"
      | "missing"
      | string;
    PriceSnapshot: {
      rate_card_hash: string;
      rate_card_id: string | null;
      resolved: boolean;
      provider: string | null;
      source_url: string | null;
      pricing_version: string | null;
      last_checked_at: string | null;
      currency: string | null;
      group: string | null;
      group_ratio: number | null;
    };
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
      total_prompt_tokens: number;
      total_completion_tokens: number;
      total_tokens?: number;
      llm_calls_count: number;
      estimated_cost_usd?: number | null;
      cost_currency?: string | null;
      cost_status?: components["schemas"]["UsageCostStatus"];
      cost_estimate_source?: string | null;
      cost_estimate_confidence?: string | null;
      pricing_modes?: components["schemas"]["UsagePricingMode"][];
      priced_llm_calls_count?: number;
      token_only_llm_calls_count?: number;
      price_unknown_llm_calls_count?: number;
      partial_usage_llm_calls_count?: number;
      missing_usage_llm_calls_count?: number;
      usage_reporting_status?: components["schemas"]["UsageReportingStatus"];
      usage_estimate_confidence?: components["schemas"]["UsageEstimateConfidence"];
      llm_evidence_status?: string;
      no_call?: boolean;
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
      owner_team?: { id: string; name: string };
      visibility?: "team" | "org" | "private";
      share_status?: "pending_scan" | "shared" | "blocked";
      source_provenance?: Record<string, unknown>[];
      atif_url: string;
      trajectory_url: string;
      atif_ready: boolean;
      trajectory_ready: boolean;
      artifacts: {
        step_name?: string;
        key: string;
        size: number;
        download_url: string;
        share_status?: "pending_scan" | "shared" | "blocked";
        blocked_reason?: string | null;
      }[];
      price_snapshots?: components["schemas"]["PriceSnapshot"][];
      debug_evidence?: components["schemas"]["DebugEvidence"];
      diagnosis?: components["schemas"]["DiagnosisReport"];
    };
    DebugEvidence: {
      schema_version: "1";
      generated_at?: string;
      entity: {
        type: "batch" | "trial" | string;
        id: string;
        team_id?: string;
        batch_id?: string | null;
      };
      lifecycle: Record<string, unknown>;
      worker?: Record<string, unknown>;
      agent?: Record<string, unknown>;
      provider?: {
        llm_calls_count?: number;
        total_prompt_tokens?: number;
        total_completion_tokens?: number;
        models?: string[];
        dialects?: string[];
        max_attempt?: number;
        latest_call_at?: string | null;
        total_cost_usd?: string;
        provider_connection_id?: string | null;
        provider_model_id?: string | null;
        [k: string]: unknown;
      };
      failure: {
        reason_code: string;
        reason?: string | null;
        category: string;
        attribution: string;
        message?: string | null;
        failure_class?: string;
        root_cause?: string;
        platform_outcome?: string;
        score_outcome?: string;
        rerun_recommendation?: string;
        rerunnable?: boolean;
        requires_operator_approval?: boolean;
        requires_task_change?: boolean;
      };
      task?: Record<string, unknown>;
      task_selection?: Record<string, unknown>;
      trials?: Record<string, unknown>;
      reward?: Record<string, unknown>;
      evidence_refs?: Record<string, unknown>;
      next_actions: string[];
    };
    DiagnosisReport: {
      schema_version: "1";
      generated_at?: string;
      entity: {
        type: "batch" | "trial" | string;
        id: string;
      };
      summary: string;
      primary_cause: {
        reason_code: string;
        category: string;
        attribution: string;
        confidence: "high" | "medium" | "low" | string;
        affected_trials: number;
        affected_ratio: number;
      };
      impact: string;
      evidence: string[];
      next_actions: {
        label: string;
        kind: "manual" | "cli_command" | "web_action" | string;
        command?: string;
        action?: string;
      }[];
      reason_clusters: {
        reason_code: string;
        category?: string;
        attribution?: string;
        count: number;
        affected_ratio?: number;
        representative_trial_id?: string | null;
        representative_task_id?: string | null;
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
      license_allowed_task_count?: number;
      license_blocked_task_count?: number;
      blocked_licenses?: string[];
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
      name: string | null;
      token_hash_prefix: string;
      type: string;
      scopes: string[];
      team_id: string | null;
      issued_at: string;
      expires_at: string | null;
      revoked_at: string | null;
      last_used_at: string | null;
      created_by_actor: string | null;
      created_by_user_id: string | null;
    };
    TokenList: { items: components["schemas"]["Token"][] };
    Batch: {
      id: string;
      team_id: string;
      owner_team?: { id: string; name: string };
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
      total_prompt_tokens: number;
      total_completion_tokens: number;
      total_tokens?: number;
      llm_calls_count: number;
      estimated_cost_usd?: number | null;
      cost_currency?: string | null;
      cost_status?: components["schemas"]["UsageCostStatus"];
      cost_estimate_source?: string | null;
      cost_estimate_confidence?: string | null;
      pricing_modes?: components["schemas"]["UsagePricingMode"][];
      priced_llm_calls_count?: number;
      token_only_llm_calls_count?: number;
      price_unknown_llm_calls_count?: number;
      partial_usage_llm_calls_count?: number;
      missing_usage_llm_calls_count?: number;
      usage_reporting_status?: components["schemas"]["UsageReportingStatus"];
      usage_estimate_confidence?: components["schemas"]["UsageEstimateConfidence"];
      budget_usd?: number | null;
      budget_policy?: string;
      budget_remaining_usd?: number | null;
      budget_consumed_usd?: number | null;
      budget_status?: string;
      pre_run_estimated_cost_usd?: number | null;
      pre_run_cost_estimate_source?: string | null;
      pre_run_cost_estimate_confidence?: string | null;
      budget_diagnostics?: Record<string, unknown>[];
      no_call_trial_count?: number;
      llm_evidence_status?: string;
      model_backed_terminal_trial_count?: number;
      visibility?: "team" | "org" | "private";
      share_status?: "pending_scan" | "shared" | "blocked";
      source_provenance?: Record<string, unknown>[];
    };
    Combination: {
      label?: string;
      agent_name: string;
      agent_model: { provider: string; name: string } | null;
      n_per_task: number;
      provider_connection_id?: string | null;
      provider_model_id?: string | null;
    };
    BatchList: {
      items: components["schemas"]["Batch"][];
      next_cursor: string | null;
    };
    BenchmarkSummary: {
      benchmark_id: string | null;
      display_name: string;
      metric_name: string;
      expected_trial_count: number;
      completed_trial_count: number;
      platform_failed_count: number;
      trial_summary: Record<string, number>;
      aggregate_reward: number | null;
    };
    RerunTarget: {
      task_id: string;
      sample_idx: number;
      combination_idx: number;
      original_trial_id: string;
      failure_reason?: string | null;
      reason_code: string;
      failure_class: string;
      root_cause: string;
      platform_outcome: string;
      score_outcome: string;
      rerun_recommendation: string;
      requires_operator_approval: boolean;
      requires_task_change: boolean;
    };
    FinalTrialSelection: {
      task_id: string;
      sample_idx: number;
      combination_idx: number;
      selected_trial_id: string;
      selected_batch_id: string | null;
      selected_source: "main" | "supplemental" | string;
      original_trial_id: string;
      original_failure_class: string;
    };
    RerunPlan: {
      schema_version: "1";
      batch_id: string;
      rerun_of_batch_id: string;
      supplemental_task_ids: string[];
      summary: {
        auto_safe: number;
        operator_approval: number;
        not_rerunnable: number;
        already_covered: number;
        selected_final_trials: number;
      };
      auto_safe: components["schemas"]["RerunTarget"][];
      operator_approval: components["schemas"]["RerunTarget"][];
      not_rerunnable: components["schemas"]["RerunTarget"][];
      final_trial_selection: components["schemas"]["FinalTrialSelection"][];
    };
    BatchDetail: components["schemas"]["Batch"] & {
      trial_summary: Record<string, number>;
      aggregate_reward: number | null;
      benchmark_summary: components["schemas"]["BenchmarkSummary"][];
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
      rerun_plan?: components["schemas"]["RerunPlan"];
      final_trial_selection?: components["schemas"]["FinalTrialSelection"][];
      effective_trial_summary: Record<string, number>;
      effective_result_status: string | null;
      effective_aggregate_reward: number | null;
      effective_total_prompt_tokens: number;
      effective_total_completion_tokens: number;
      effective_llm_calls_count: number;
      effective_total_tokens?: number;
      effective_estimated_cost_usd?: number | null;
      effective_cost_currency?: string | null;
      effective_cost_status?: components["schemas"]["UsageCostStatus"];
      effective_pricing_modes?: components["schemas"]["UsagePricingMode"][];
      effective_priced_llm_calls_count?: number;
      effective_token_only_llm_calls_count?: number;
      effective_price_unknown_llm_calls_count?: number;
      effective_partial_usage_llm_calls_count?: number;
      effective_missing_usage_llm_calls_count?: number;
      effective_usage_reporting_status?: components["schemas"]["UsageReportingStatus"];
      effective_usage_estimate_confidence?: components["schemas"]["UsageEstimateConfidence"];
      effective_no_call_trial_count?: number;
      effective_llm_evidence_status?: string;
      effective_model_backed_terminal_trial_count?: number;
      price_snapshots?: components["schemas"]["PriceSnapshot"][];
      effective_price_snapshots?: components["schemas"]["PriceSnapshot"][];
      debug_evidence?: components["schemas"]["DebugEvidence"];
      diagnosis?: components["schemas"]["DiagnosisReport"];
    };
    DeliveryExport: {
      id?: string;
      status: "ready" | "not_ready" | string;
      reason?: string | null;
      archive_filename?: string;
      sha256?: string;
      download_url?: string;
      manifest?: {
        task_count?: number;
        trial_count?: number;
        reward_distribution?: Record<string, number>;
        object_counts?: Record<string, number>;
        archive_sha256?: string | null;
        payload_checksums?: {
          algorithm?: string;
          file?: string;
          scope?: string;
        };
        [key: string]: unknown;
      };
      object_validation?: {
        checked?: number;
        missing?: unknown[];
      };
      storage?: {
        bucket?: string | null;
        key?: string | null;
        size_bytes?: number | null;
      };
      created_at?: string | null;
    };
    UsageBatch: {
      batch_id: string;
      batch_name: string;
      team_id: string;
      team_name?: string | null;
      trial_count: number;
      llm_input_tokens: number;
      llm_output_tokens: number;
      estimated_cost_usd: number | null;
      cost_currency: string | null;
      cost_status: components["schemas"]["UsageCostStatus"];
      cost_estimate_source?: string | null;
      cost_estimate_confidence?: string | null;
      pricing_modes: components["schemas"]["UsagePricingMode"][];
      priced_llm_calls_count?: number;
      token_only_llm_calls_count?: number;
      price_unknown_llm_calls_count?: number;
      partial_usage_llm_calls_count?: number;
      missing_usage_llm_calls_count?: number;
      usage_reporting_status?: components["schemas"]["UsageReportingStatus"];
      usage_estimate_confidence?: components["schemas"]["UsageEstimateConfidence"];
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
      estimated_cost_usd?: number | null;
      cost_currency?: string | null;
      cost_status?: components["schemas"]["UsageCostStatus"];
      cost_estimate_source?: string | null;
      cost_estimate_confidence?: string | null;
      pricing_modes?: components["schemas"]["UsagePricingMode"][];
      priced_llm_calls_count?: number;
      token_only_llm_calls_count?: number;
      price_unknown_llm_calls_count?: number;
      partial_usage_llm_calls_count?: number;
      missing_usage_llm_calls_count?: number;
      usage_reporting_status?: components["schemas"]["UsageReportingStatus"];
      usage_estimate_confidence?: components["schemas"]["UsageEstimateConfidence"];
      llm_input_tokens: number;
      llm_output_tokens: number;
      batches?: components["schemas"]["UsageBatch"][];
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
    TeamUserMember: {
      user_id: string;
      email: string;
      display_name: string | null;
      role: string;
      joined_at: string;
    };
    TeamQuota: {
      fair_share_weight: number;
      max_attempts_ceiling: number;
      in_flight_count: number;
      license_allowlist: string[];
    };
    Team: {
      id: string;
      name: string;
      created_at: string;
      quota: components["schemas"]["TeamQuota"] | null;
      members: components["schemas"]["TeamMember"][];
      user_members?: components["schemas"]["TeamUserMember"][];
    };
  };
}
