import type { ComponentType } from "react";

import type { PipelineArtifactDetail } from "../../api/client";
import BehaviorRolloutViewer from "./BehaviorRolloutViewer";
import GenericArtifactViewer from "./GenericArtifactViewer";

export type ArtifactViewerProps = { artifact: PipelineArtifactDetail };

export const ARTIFACT_RENDERERS: Readonly<
  Record<string, ComponentType<ArtifactViewerProps>>
> = Object.freeze({
  "behavior_rollout_bundle.v1": BehaviorRolloutViewer,
});

export default function ArtifactRenderer(props: ArtifactViewerProps): JSX.Element {
  const Renderer = ARTIFACT_RENDERERS[props.artifact.artifact_type] ?? GenericArtifactViewer;
  return <Renderer {...props} />;
}
