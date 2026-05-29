from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable

from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session

from agentic_data_platform.persistence.repositories import (
    AuditEventRepository,
    IdentityRepository,
    ProjectRecord,
    ProjectRepository,
    TeamRecord,
)
from agentic_data_platform.service.security import (
    accessible_project_ids,
    require_authenticated_user,
    require_project_role,
)


class ProjectUpdateRequest(BaseModel):
    name: str | None = None
    description: str | None = None
    status: str | None = None


def register_project_routes(app: FastAPI, session_dependency: Callable) -> None:
    @app.get("/teams", tags=["projects"], responses=_example_response(_TEAMS_EXAMPLE))
    def list_teams(request: Request, session: Session = Depends(session_dependency)) -> dict:
        require_authenticated_user(request, session)
        teams = IdentityRepository(session).list_teams()
        return {"teams": [_team_payload(team) for team in teams], "request_id": _request_id(request)}

    @app.get("/teams/{team_id}", tags=["projects"], responses=_example_response(_TEAM_EXAMPLE))
    def get_team(team_id: str, request: Request, session: Session = Depends(session_dependency)) -> dict:
        require_authenticated_user(request, session)
        try:
            team = IdentityRepository(session).get_team(team_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Team not found") from exc
        return {"team": _team_payload(team), "request_id": _request_id(request)}

    @app.get("/projects", tags=["projects"], responses=_example_response(_PROJECTS_EXAMPLE))
    def list_projects(
        request: Request,
        owner_team_id: str | None = None,
        session: Session = Depends(session_dependency),
    ) -> dict:
        auth = require_authenticated_user(request, session)
        allowed_project_ids = accessible_project_ids(session, auth)
        projects = [
            project
            for project in ProjectRepository(session).list_projects(owner_team_id=owner_team_id)
            if project.project_id in allowed_project_ids
        ]
        return {"projects": [_project_payload(project) for project in projects], "request_id": _request_id(request)}

    @app.get("/projects/{project_id}", tags=["projects"], responses=_example_response(_PROJECT_EXAMPLE))
    def get_project(project_id: str, request: Request, session: Session = Depends(session_dependency)) -> dict:
        auth = require_authenticated_user(request, session)
        project = require_project_role(session, auth, project_id, minimum_role="viewer")
        return {"project": _project_payload(project), "request_id": _request_id(request)}

    @app.patch("/projects/{project_id}", tags=["projects"], responses=_example_response(_PROJECT_EXAMPLE))
    def update_project(
        project_id: str,
        updates: ProjectUpdateRequest,
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> dict:
        auth = require_authenticated_user(request, session)
        require_project_role(session, auth, project_id, minimum_role="member")
        try:
            project = ProjectRepository(session).update_project(
                project_id=project_id,
                name=updates.name,
                description=updates.description,
                status=updates.status,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Project not found") from exc
        AuditEventRepository(session).record_event(
            event_type="project.updated",
            actor_user_id=auth.user.user_id,
            project_id=project_id,
            subject_type="project",
            subject_id=project_id,
            payload={
                "name": updates.name,
                "description": updates.description,
                "status": updates.status,
            },
            request_id=_request_id(request),
        )
        return {"project": _project_payload(project), "request_id": _request_id(request)}


def _team_payload(team: TeamRecord) -> dict:
    return {
        "team_id": team.team_id,
        "name": team.name,
        "created_at": _iso_z(team.created_at),
    }


def _project_payload(project: ProjectRecord) -> dict:
    return {
        "project_id": project.project_id,
        "name": project.name,
        "owner_team_id": project.owner_team_id,
        "created_by_user_id": project.created_by_user_id,
        "description": project.description,
        "status": project.status,
        "created_at": _iso_z(project.created_at),
        "updated_at": _iso_z(project.updated_at),
    }


def _iso_z(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _request_id(request: Request) -> str:
    return getattr(request.state, "request_id", "")


def _example_response(example: dict) -> dict:
    return {200: {"content": {"application/json": {"example": example}}}}


_TEAM_PAYLOAD_EXAMPLE = {
    "team_id": "pilot-project",
    "name": "pilot group",
    "created_at": "2026-05-28T12:00:00Z",
}
_PROJECT_PAYLOAD_EXAMPLE = {
    "project_id": "latent-skill-pilot",
    "name": "Latent Skill Pilot",
    "owner_team_id": "pilot-project",
    "created_by_user_id": "[REDACTED_OWNER]",
    "description": "SkillFlow and SkillLearnBench pilot",
    "status": "active",
    "created_at": "2026-05-28T12:00:00Z",
    "updated_at": "2026-05-28T12:00:00Z",
}
_TEAMS_EXAMPLE = {"teams": [_TEAM_PAYLOAD_EXAMPLE], "request_id": "req_123"}
_TEAM_EXAMPLE = {"team": _TEAM_PAYLOAD_EXAMPLE, "request_id": "req_123"}
_PROJECTS_EXAMPLE = {"projects": [_PROJECT_PAYLOAD_EXAMPLE], "request_id": "req_123"}
_PROJECT_EXAMPLE = {"project": _PROJECT_PAYLOAD_EXAMPLE, "request_id": "req_123"}
