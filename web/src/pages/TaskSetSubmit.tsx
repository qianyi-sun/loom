import { useMutation } from "@tanstack/react-query";
import { useRef, useState } from "react";
import { useNavigate } from "react-router-dom";

import { api, type ApiError, type TaskSetSubmitResponse } from "../api/client";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import DocsCallout from "../components/DocsCallout";

export default function TaskSetSubmit(): JSX.Element {
  const navigate = useNavigate();
  const [error, setError] = useState<string | null>(null);
  const manifestRef = useRef<HTMLInputElement>(null);
  const verifierRef = useRef<HTMLInputElement>(null);
  const transformRef = useRef<HTMLInputElement>(null);

  const submit = useMutation({
    mutationFn: (formData: FormData) => api.submitTaskSet(formData),
    onSuccess: (res: TaskSetSubmitResponse) => {
      navigate(`/task-sets/${encodeURIComponent(res.task_set_id)}`);
    },
    onError: (err: unknown) => {
      const apiErr = err as ApiError | undefined;
      setError(apiErr?.detail ?? "Submission failed. Please try again.");
    },
  });

  const handleSubmit = (e: React.FormEvent): void => {
    e.preventDefault();
    setError(null);

    const manifestFile = manifestRef.current?.files?.[0];
    if (!manifestFile) {
      setError("A manifest file is required.");
      return;
    }

    const formData = new FormData();
    formData.append("manifest", manifestFile);

    const verifierFile = verifierRef.current?.files?.[0];
    if (verifierFile) formData.append("verifier", verifierFile);

    const transformFile = transformRef.current?.files?.[0];
    if (transformFile) formData.append("transform", transformFile);

    submit.mutate(formData);
  };

  return (
    <div className="space-y-4">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">Submit Task Set</h1>
        <p className="text-sm text-slate-500">
          Upload a manifest and optional verifier/transform scripts.
        </p>
      </header>

      <Card>
        <Card.Body>
          <form onSubmit={handleSubmit} className="space-y-5">
            <div>
              <label className="block text-sm font-medium text-slate-700">
                Manifest (required)
              </label>
              <input
                ref={manifestRef}
                type="file"
                accept=".yaml,.yml,.json"
                className="mt-1 block w-full text-sm text-slate-600 file:mr-3 file:rounded-md file:border file:border-slate-200 file:bg-white file:px-3 file:py-1.5 file:text-sm file:font-medium file:text-slate-700 hover:file:bg-slate-50"
              />
              <p className="mt-1 text-xs text-slate-500">
                YAML or JSON manifest describing the task set.
              </p>
            </div>

            <div>
              <label className="block text-sm font-medium text-slate-700">
                Verifier (optional)
              </label>
              <input
                ref={verifierRef}
                type="file"
                accept=".py"
                className="mt-1 block w-full text-sm text-slate-600 file:mr-3 file:rounded-md file:border file:border-slate-200 file:bg-white file:px-3 file:py-1.5 file:text-sm file:font-medium file:text-slate-700 hover:file:bg-slate-50"
              />
              <p className="mt-1 text-xs text-slate-500">
                Required for evaluation intent. Python script that scores agent output.
              </p>
            </div>

            <div>
              <label className="block text-sm font-medium text-slate-700">
                Transform (optional)
              </label>
              <input
                ref={transformRef}
                type="file"
                accept=".py"
                className="mt-1 block w-full text-sm text-slate-600 file:mr-3 file:rounded-md file:border file:border-slate-200 file:bg-white file:px-3 file:py-1.5 file:text-sm file:font-medium file:text-slate-700 hover:file:bg-slate-50"
              />
              <p className="mt-1 text-xs text-slate-500">
                Optional script to transform upstream data rows before task rendering.
              </p>
            </div>

            {error ? (
              <div className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
                {error}
              </div>
            ) : null}

            <div className="flex items-center gap-3">
              <Button
                type="submit"
                variant="primary"
                disabled={submit.isPending}
              >
                {submit.isPending ? "Uploading..." : "Submit Task Set"}
              </Button>
              <Button
                variant="secondary"
                onClick={() => navigate("/task-sets")}
              >
                Cancel
              </Button>
            </div>
          </form>
        </Card.Body>
      </Card>

      <DocsCallout title="Manifest YAML schema" tone="info">
        <p>
          The manifest file must conform to <code>apiVersion: loom.taskset/v1</code>.
          Required top-level fields:
        </p>
        <pre className="mt-2 overflow-x-auto rounded bg-blue-100/50 p-2 text-xs leading-relaxed">
{`apiVersion: loom.taskset/v1
kind: UserTaskSet

metadata:
  name: my-coding-tasks        # slug identifier
  display_name: My Coding Tasks

intents:
  - trajectory_generation      # always allowed
  - evaluation                 # requires verifier

source:
  type: hf                     # hf | git | https | jsonl-inline
  locator: namespace/dataset
  revision: 1.2.3             # optional
  subset: default             # optional
  split: test                 # optional

instance_mapping:
  prompt: row.question
  answer: row.solution
  task_id: row.id

task_template:
  task:
    id: "{{ instance.task_id }}"
    name: "{{ metadata.display_name }} - {{ instance.task_id }}"
  environment:
    os: linux
    docker_image: ghcr.io/example/coding-task:1.0
  agent:
    name: default
  steps:
    - artifacts: [solution.py]

verifier:                      # optional; required for evaluation
  type: pytest                 # pytest | script | exact-match | regex | llm-judge
  file: verifier/test_solution.py

transform:                     # optional
  file: transform.py

limits:
  max_instances: 500
  timeout_per_task_s: 300`}
        </pre>
        <p className="mt-2 text-xs text-slate-500">
          <code>evaluation</code> intent requires a verifier file.
          A manifest without explicit intents defaults to <code>trajectory_generation</code>.
        </p>
      </DocsCallout>
    </div>
  );
}
