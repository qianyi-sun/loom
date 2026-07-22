import { expectTypeOf, test } from "vitest";

import type { components } from "../api/schema";

test("the checked-in API contract owns frontend-visible wire fields", () => {
  type Task = components["schemas"]["Task"];
  type Team = components["schemas"]["Team"];
  type Trial = components["schemas"]["TrialDetail"];

  expectTypeOf<Task["name"]>().toEqualTypeOf<string | null>();
  expectTypeOf<Task["description"]>().toEqualTypeOf<string | null>();
  expectTypeOf<Task["agent_name"]>().toEqualTypeOf<string | null>();
  expectTypeOf<Task["verifier_name"]>().toEqualTypeOf<string | null>();
  expectTypeOf<Task["step_count"]>().toEqualTypeOf<number>();
  expectTypeOf<Team["disabled_at"]>().toEqualTypeOf<string | null | undefined>();
  expectTypeOf<Trial["submitted_by_user"]>().toEqualTypeOf<{
    id: string;
    username: string;
    team_id?: string | null;
    team_name?: string | null;
  } | null | undefined>();
});
