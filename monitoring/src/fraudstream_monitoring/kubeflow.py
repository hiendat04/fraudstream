"""The few calls the retrain step makes to the Kubeflow Pipelines API (v2beta1)."""

import json

import httpx

API = "/apis/v2beta1"
# The API's number for EQUALS, the same one the kfp SDK sends.
EQUALS = 1


def _named(name: str) -> str:
    return json.dumps(
        {"predicates": [{"operation": EQUALS, "key": "display_name", "stringValue": name}]}
    )


class Pipelines:
    def __init__(self, client: httpx.Client):
        self.http = client

    def _get(self, path: str, **params) -> dict:
        answer = self.http.get(f"{API}{path}", params=params)
        answer.raise_for_status()
        return answer.json()

    def _post(self, path: str, body: dict) -> dict:
        answer = self.http.post(f"{API}{path}", json=body)
        answer.raise_for_status()
        return answer.json()

    def pipeline_id(self, name: str) -> str:
        found = self._get("/pipelines", filter=_named(name)).get("pipelines", [])
        if not found:
            raise LookupError(f"no pipeline called {name!r}: CI registers it when it deploys training")
        return found[0]["pipeline_id"]

    def newest_version_id(self, pipeline_id: str) -> str:
        found = self._get(
            f"/pipelines/{pipeline_id}/versions", sort_by="created_at desc", page_size=1
        ).get("pipeline_versions", [])
        if not found:
            raise LookupError(f"pipeline {pipeline_id} has no versions")
        return found[0]["pipeline_version_id"]

    def experiment_id(self, name: str) -> str:
        found = self._get("/experiments", filter=_named(name)).get("experiments", [])
        if found:
            return found[0]["experiment_id"]
        return self._post("/experiments", {"display_name": name})["experiment_id"]

    def recent_runs(self, experiment_id: str, count: int = 20) -> list[dict]:
        return self._get(
            "/runs", experiment_id=experiment_id, sort_by="created_at desc", page_size=count
        ).get("runs", [])

    def start_run(
        self, *, name: str, experiment_id: str, pipeline_id: str, version_id: str, parameters: dict
    ) -> str:
        return self._post(
            "/runs",
            {
                "display_name": name,
                "experiment_id": experiment_id,
                "pipeline_version_reference": {
                    "pipeline_id": pipeline_id,
                    "pipeline_version_id": version_id,
                },
                "runtime_config": {"parameters": parameters},
            },
        )["run_id"]
