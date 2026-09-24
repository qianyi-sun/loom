/** Task-source catalog shape shared by selection and rendering. */

export interface BenchmarkItem {
  id: string;
  display_name?: string;
  detail_label?: string;
  task_count?: number;
  raw_task_count?: number;
  valid_task_config_count?: number;
  invalid_task_config_count?: number;
  license_allowed_task_count?: number;
  license_blocked_task_count?: number;
  blocked_licenses?: string[];
  readiness_state?: string;
  readiness_label?: string;
  readiness_message?: string | null;
  selectable?: boolean;
  blocker_reason?: string | null;
  series?: string | null;
}

const TASK_SET_ID_PREFIX = "ts/";
export function isTaskSetId(id: string): boolean {
  return id.startsWith(TASK_SET_ID_PREFIX);
}
