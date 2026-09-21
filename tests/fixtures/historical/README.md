# Synthetic historical records

These fixtures preserve migration and retained-data coverage after shared-cluster
runtime retirement. They are test data, not deployment inputs or credentials.

Task-image and capacity-claim rows were captured from `020a6e928` in disposable
PostgreSQL. `capacity_observation_binding.json` and `personal_build_request.json`
were captured from the unchanged legacy test issuers at `201d9e64b`. The latter
includes the standard migration-seeded users with null passwords and a synthetic
`example.test` owner. No live database or signing key was used.

Fixture restore helpers temporarily bypass triggers only during disposable test
setup. Assertions run with normal triggers, constraints and role permissions.
Published migrations remain the authority for historical schema structure.
