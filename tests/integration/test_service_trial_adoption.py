"""Service adoption authorization and preservation of the operation identity."""

import json
from uuid import uuid4

import httpx

from tests.integration.test_service_trials_read import trials_setup  # noqa: F401


async def test_service_adoption_checks_authentication_before_forwarding(trials_setup):  # noqa: F811
    app, token, _team, trials = trials_setup
    observed = []
    operation_id = str(uuid4())

    def upstream(request):
        observed.append(request)
        return httpx.Response(200, json={"trial_id": str(trials[0]), "ready": True})

    await app.state.http_client.aclose()
    app.state.http_client = httpx.AsyncClient(base_url="http://cp", transport=httpx.MockTransport(upstream))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://service") as client:
        path = f"/api/v1/trials/{trials[0]}/adopt-protected"
        body = {"operation_id": operation_id}
        assert (await client.post(path, json=body)).status_code == 401
        assert observed == []
        headers = {"Authorization": f"Bearer {token}"}
        assert (await client.post(f"/api/v1/trials/{uuid4()}/adopt-protected", json=body, headers=headers)).status_code == 404
        assert observed == []
        assert (await client.post(path, json=body | {"team_id": str(uuid4())}, headers=headers)).status_code == 422
        assert observed == []
        response = await client.post(path, json=body, headers=headers)
        assert response.status_code == 200, response.text
        assert response.json() == {"trial_id": str(trials[0]), "ready": True}
        assert len(observed) == 1
        assert observed[0].url.path == f"/trials/{trials[0]}/adopt-protected"
        assert json.loads(observed[0].content) == body
        assert observed[0].headers["Authorization"] == headers["Authorization"]
