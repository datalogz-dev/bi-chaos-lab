from __future__ import annotations

import os
import time
import urllib.parse
import uuid
from dataclasses import dataclass

from bi_chaos_lab.http import request_bytes, request_form, request_json
from bi_chaos_lab.manifest import Manifest
from bi_chaos_lab.providers.base import Provider
from bi_chaos_lab.scenario_engine import AssetPlan, SeedPlan
from bi_chaos_lab.state import StateFile, TrackedObject


@dataclass
class _Token:
    access_token: str
    expires_at: float


class PowerBIProvider(Provider):
    name = "powerbi"
    api_base = "https://api.powerbi.com/v1.0/myorg"
    token_url = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    scope = "https://analysis.windows.net/powerbi/api/.default"

    def __init__(self, manifest: Manifest, state: StateFile) -> None:
        super().__init__(manifest, state)
        self._token: _Token | None = None

    @property
    def enabled(self) -> bool:
        return self.manifest.platforms.powerbi.enabled

    def validate(self) -> None:
        if not self.enabled:
            return
        self._get_token(force=True)

    def seed(self, plan: SeedPlan, *, dry_run: bool) -> None:
        if not self.enabled:
            return
        if not dry_run:
            self._reconcile_existing_state(plan)
        for workspace in plan.powerbi_workspaces:
            existing = self._find_tracked("workspace", workspace["name"])
            if existing:
                continue
            if dry_run:
                self.state.record_event("would-create", self.name, workspace["name"], "dry-run")
                continue
            workspace_id = self._create_workspace(workspace["name"])
            self.state.add(
                TrackedObject(
                    platform=self.name,
                    kind="workspace",
                    name=workspace["name"],
                    external_id=workspace_id,
                    domain=workspace["domain"],
                    team=workspace["team"],
                )
            )
            self.state.record_event("create", self.name, workspace["name"], workspace_id)

        powerbi_assets = [asset for asset in plan.assets if asset.platform == self.name]
        family_map = {family.name: family for family in self.manifest.template_families if family.platform == self.name}
        for asset in powerbi_assets:
            workspace = self._find_tracked("workspace", asset.container_name)
            if workspace and self._asset_exists_in_workspace(asset.asset_name, workspace.external_id):
                continue
            if dry_run:
                self.state.record_event("would-import", self.name, asset.asset_name, "dry-run")
                continue
            family = family_map[asset.template_family]
            if workspace is None:
                raise RuntimeError(f"workspace was not created: {asset.container_name}")
            imported_ids = self._import_pbix(workspace.external_id, asset.asset_name, str(self.template_path(family.path)))
            for kind, external_id in imported_ids.items():
                self.state.add_or_update(
                    TrackedObject(
                        platform=self.name,
                        kind=kind,
                        name=asset.asset_name,
                        external_id=external_id,
                        parent_external_id=workspace.external_id,
                        domain=asset.domain,
                        team=asset.team,
                        template_family=asset.template_family,
                        source_ref=asset.source_ref,
                        tags=list(asset.tags),
                    )
                )
                self.state.record_event("create", self.name, asset.asset_name, external_id)

    def evolve(self, plan: list[AssetPlan], *, dry_run: bool) -> None:
        if not self.enabled:
            return
        datasets = self.state.find(platform=self.name, kind="dataset")
        for index, dataset in enumerate(datasets):
            if "stale" in dataset.tags or index % 3 != 0:
                continue
            if dry_run:
                self.state.record_event("would-refresh", self.name, dataset.name, dataset.external_id)
                continue
            self._trigger_refresh(dataset.parent_external_id or "", dataset.external_id)
            self.state.record_event("refresh", self.name, dataset.name, dataset.external_id)

    def teardown(self, *, dry_run: bool) -> None:
        if not self.enabled:
            return
        workspaces = list(reversed(self.state.find(platform=self.name, kind="workspace")))
        for workspace in workspaces:
            if self.manifest.safety.teardown_requires_prefix_match and not workspace.name.startswith(
                self.manifest.safety.workspace_prefix
            ):
                raise RuntimeError(f"refusing to delete workspace outside prefix: {workspace.name}")
            if dry_run:
                self.state.record_event("would-delete", self.name, workspace.name, workspace.external_id)
                continue
            self._delete_workspace(workspace.external_id)
            self.state.record_event("delete", self.name, workspace.name, workspace.external_id)

    def _find_tracked(self, kind: str, name: str, parent_external_id: str | None = None) -> TrackedObject | None:
        return self.state.find_one(
            platform=self.name,
            kind=kind,
            name=name,
            parent_external_id=parent_external_id,
        )

    def _asset_exists_in_workspace(self, name: str, workspace_id: str) -> bool:
        return any(
            self._find_tracked(kind, name, workspace_id) is not None
            for kind in ("dataset", "report", "import")
        )

    def _headers(self) -> dict[str, str]:
        token = self._get_token(force=False)
        return {"Authorization": f"Bearer {token}"}

    def _get_token(self, *, force: bool) -> str:
        if not force and self._token and time.time() < self._token.expires_at - 300:
            return self._token.access_token
        cfg = self.manifest.platforms.powerbi
        response = request_form(
            "POST",
            self.token_url.format(tenant_id=cfg.tenant_id()),
            form={
                "grant_type": "client_credentials",
                "client_id": cfg.client_id(),
                "client_secret": cfg.client_secret(),
                "scope": self.scope,
            },
        ).json()
        token = str(response["access_token"])
        expires_in = int(response.get("expires_in", 3600))
        self._token = _Token(access_token=token, expires_at=time.time() + expires_in)
        return token

    def _create_workspace(self, name: str) -> str:
        payload = {"name": name}
        response = request_json(
            "POST",
            f"{self.api_base}/groups?workspaceV2=true",
            headers=self._headers(),
            body=payload,
        ).json()
        return str(response["id"])

    def _reconcile_existing_state(self, plan: SeedPlan) -> None:
        groups = self._list_workspaces()
        group_by_name = {group["name"]: group for group in groups}
        for workspace in plan.powerbi_workspaces:
            existing = group_by_name.get(workspace["name"])
            if not existing:
                continue
            self.state.add_or_update(
                TrackedObject(
                    platform=self.name,
                    kind="workspace",
                    name=workspace["name"],
                    external_id=str(existing["id"]),
                    domain=workspace["domain"],
                    team=workspace["team"],
                )
            )

        for asset in [item for item in plan.assets if item.platform == self.name]:
            workspace = self._find_tracked("workspace", asset.container_name)
            if workspace is None:
                continue
            reports, datasets = self._list_workspace_content(workspace.external_id)
            for report in reports:
                if report.get("name") == asset.asset_name:
                    self.state.add_or_update(
                        TrackedObject(
                            platform=self.name,
                            kind="report",
                            name=asset.asset_name,
                            external_id=str(report["id"]),
                            parent_external_id=workspace.external_id,
                            domain=asset.domain,
                            team=asset.team,
                            template_family=asset.template_family,
                            source_ref=asset.source_ref,
                            tags=list(asset.tags),
                        )
                    )
            for dataset in datasets:
                if dataset.get("name") == asset.asset_name:
                    self.state.add_or_update(
                        TrackedObject(
                            platform=self.name,
                            kind="dataset",
                            name=asset.asset_name,
                            external_id=str(dataset["id"]),
                            parent_external_id=workspace.external_id,
                            domain=asset.domain,
                            team=asset.team,
                            template_family=asset.template_family,
                            source_ref=asset.source_ref,
                            tags=list(asset.tags),
                        )
                    )

    def _list_workspaces(self) -> list[dict[str, object]]:
        response = request_json(
            "GET",
            f"{self.api_base}/groups?$top=5000",
            headers=self._headers(),
        ).json()
        return list((response or {}).get("value", []))

    def _list_workspace_content(self, workspace_id: str) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        reports = request_json(
            "GET",
            f"{self.api_base}/groups/{workspace_id}/reports",
            headers=self._headers(),
        ).json()
        datasets = request_json(
            "GET",
            f"{self.api_base}/groups/{workspace_id}/datasets",
            headers=self._headers(),
        ).json()
        return list((reports or {}).get("value", [])), list((datasets or {}).get("value", []))

    def _import_pbix(self, workspace_id: str, display_name: str, template_path: str) -> dict[str, str]:
        content = open(template_path, "rb").read()
        dataset_name = f"{display_name}.pbix"
        quoted_name = urllib.parse.quote(dataset_name)
        url = (
            f"{self.api_base}/groups/{workspace_id}/imports"
            f"?datasetDisplayName={quoted_name}&nameConflict=CreateOrOverwrite"
        )
        boundary = f"----bi-chaos-lab-{uuid.uuid4().hex}"
        filename = os.path.basename(template_path)
        body = bytearray()
        body.extend(f"--{boundary}\r\n".encode("utf-8"))
        body.extend(
            f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'.encode("utf-8")
        )
        body.extend(b"Content-Type: application/octet-stream\r\n\r\n")
        body.extend(content)
        body.extend(b"\r\n")
        body.extend(f"--{boundary}--\r\n".encode("utf-8"))
        response = request_bytes(
            "POST",
            url,
            headers={
                **self._headers(),
                "Content-Type": f"multipart/form-data; boundary={boundary}",
            },
            body=bytes(body),
            timeout=300,
        ).json()
        if response and response.get("id") and not ((response.get("datasets") or []) or (response.get("reports") or [])):
            response = self._wait_for_import(workspace_id, str(response["id"]))
        dataset_id = ""
        report_id = ""
        if response:
            datasets = response.get("datasets") or []
            reports = response.get("reports") or []
            if datasets:
                dataset_id = str(datasets[0]["id"])
            if reports:
                report_id = str(reports[0]["id"])
        result: dict[str, str] = {}
        if dataset_id:
            result["dataset"] = dataset_id
        if report_id:
            result["report"] = report_id
        if not result:
            result["import"] = str(response["id"])
        return result

    def _wait_for_import(self, workspace_id: str, import_id: str, timeout_seconds: int = 180) -> dict[str, object]:
        deadline = time.time() + timeout_seconds
        last_response: dict[str, object] = {"id": import_id}
        while time.time() < deadline:
            response = request_json(
                "GET",
                f"{self.api_base}/groups/{workspace_id}/imports/{import_id}",
                headers=self._headers(),
            ).json()
            if response:
                last_response = response
            state = str((response or {}).get("importState", ""))
            if state.lower() in {"succeeded", "failed"}:
                return response
            time.sleep(2)
        return last_response

    def _trigger_refresh(self, workspace_id: str, dataset_id: str) -> None:
        if not workspace_id:
            return
        request_json(
            "POST",
            f"{self.api_base}/groups/{workspace_id}/datasets/{dataset_id}/refreshes",
            headers=self._headers(),
            body={"notifyOption": "NoNotification"},
        )

    def _delete_workspace(self, workspace_id: str) -> None:
        request_json(
            "DELETE",
            f"{self.api_base}/groups/{workspace_id}",
            headers=self._headers(),
        )
