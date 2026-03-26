from __future__ import annotations

import os
import random
import time
import urllib.parse
import uuid
from dataclasses import dataclass

from bi_chaos_lab.http import HTTPError, request_bytes, request_form, request_json
from bi_chaos_lab.manifest import Manifest
from bi_chaos_lab.providers.base import Provider
from bi_chaos_lab.scenario_engine import AssetPlan, SeedPlan
from bi_chaos_lab.state import StateFile, TrackedObject

# -- Evolve chaos constants --------------------------------------------------

_PBI_EVOLVE_SUFFIXES = [
    "FINAL", "FINAL v2", "Copy", "Copy (2)", "DO NOT DELETE",
    "Old", "Updated", "BACKUP", "test", "Sandbox",
    "for Review", "DRAFT", "WIP", "Archived", "Promoted",
]

_PBI_PHANTOM_EMAILS = [
    "svc-analytics@yourorg.com",
    "data-migration-bot@yourorg.com",
    "bi-consultant-ext@partner.com",
    "intern-temp-2024@yourorg.com",
    "powerbi-gateway-svc@yourorg.com",
    "shared-reports-dl@yourorg.com",
    "finance-all@yourorg.com",
    "contractors-bi@external.com",
    "legacy-etl-svc@yourorg.com",
    "test-user-qa@yourorg.com",
]

_PBI_PHANTOM_ROLES = ["Admin", "Member", "Contributor", "Viewer"]

_PBI_SCHEDULE_DAYS = [
    "Monday", "Tuesday", "Wednesday", "Thursday",
    "Friday", "Saturday", "Sunday",
]

_PBI_SCHEDULE_TIMES = [
    "00:30", "02:00", "03:30", "04:00", "05:00",
    "06:30", "11:00", "14:30", "17:00", "23:00", "23:30",
]


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

    def seed(self, plan: SeedPlan, *, dry_run: bool, state_path: str | None = None) -> None:
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
        _checkpoint_counter = 0
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

            if asset.relationship_role == "base" and asset.kind == "dataset":
                # Shared dataset: import PBIX, keep dataset, delete auto-created report
                imported_ids = self._import_pbix(workspace.external_id, asset.asset_name, str(self.template_path(family.path)))
                dataset_id = imported_ids.get("dataset", "")
                if dataset_id:
                    self.state.add_or_update(
                        TrackedObject(
                            platform=self.name,
                            kind="dataset",
                            name=asset.asset_name,
                            external_id=dataset_id,
                            parent_external_id=workspace.external_id,
                            domain=asset.domain,
                            team=asset.team,
                            template_family=asset.template_family,
                            source_ref=asset.source_ref,
                            tags=list(asset.tags),
                        )
                    )
                    self.state.record_event("create", self.name, asset.asset_name, dataset_id)
                report_id = imported_ids.get("report", "")
                if report_id:
                    try:
                        self._delete_report(workspace.external_id, report_id)
                    except Exception:
                        pass  # orphan report is harmless

            elif asset.relationship_role == "dependent" and asset.depends_on:
                # Thin report: import PBIX then rebind to shared dataset
                base_dataset = self._find_base_dataset(asset.depends_on)
                imported_ids = self._import_pbix(workspace.external_id, asset.asset_name, str(self.template_path(family.path)))
                report_id = imported_ids.get("report", "")
                orphan_dataset_id = imported_ids.get("dataset", "")
                if report_id and base_dataset:
                    try:
                        self._rebind_report(workspace.external_id, report_id, base_dataset.external_id)
                    except Exception:
                        pass  # rebind failure is non-fatal, report still works with its own dataset
                    linked = [base_dataset.external_id]
                else:
                    linked = []
                if report_id:
                    self.state.add_or_update(
                        TrackedObject(
                            platform=self.name,
                            kind="report",
                            name=asset.asset_name,
                            external_id=report_id,
                            parent_external_id=workspace.external_id,
                            domain=asset.domain,
                            team=asset.team,
                            template_family=asset.template_family,
                            source_ref=asset.source_ref,
                            tags=list(asset.tags),
                            linked_to=linked,
                        )
                    )
                    self.state.record_event("create", self.name, asset.asset_name, report_id)
                # Delete orphan dataset from the import (the thin report now uses the shared one)
                if orphan_dataset_id and base_dataset:
                    try:
                        self._delete_dataset(workspace.external_id, orphan_dataset_id)
                    except Exception:
                        pass  # orphan dataset is harmless

            else:
                # Standard: import PBIX, track both dataset and report
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

            _checkpoint_counter += 1
            if _checkpoint_counter % 20 == 0:
                self._save_checkpoint(state_path)

    def evolve(self, plan: list[AssetPlan], *, dry_run: bool, state_path: str | None = None) -> None:
        if not self.enabled:
            return
        scenarios = self.manifest.scenarios
        rng = random.Random(self.manifest.random_seed + len(self.state.events))

        datasets = self.state.find(platform=self.name, kind="dataset")
        reports = self.state.find(platform=self.name, kind="report")
        workspaces = self.state.find(platform=self.name, kind="workspace")

        # -- 1. Refresh bursts ------------------------------------------------
        for dataset in datasets:
            if "stale" in dataset.tags:
                continue
            if rng.random() >= scenarios.refresh_failure_rate:
                continue
            if dry_run:
                self.state.record_event("would-refresh", self.name, dataset.name, dataset.external_id)
                continue
            try:
                self._trigger_refresh(dataset.parent_external_id or "", dataset.external_id)
                self.state.record_event("refresh", self.name, dataset.name, dataset.external_id)
            except Exception:
                self.state.record_event("refresh-failed", self.name, dataset.name, dataset.external_id)

        # -- 2. Ownership drift — SP takes over datasets ----------------------
        for dataset in datasets:
            if rng.random() >= scenarios.ownership_drift_rate:
                continue
            if dry_run:
                self.state.record_event("would-takeover", self.name, dataset.name, dataset.external_id)
                continue
            try:
                self._take_over_dataset(dataset.parent_external_id or "", dataset.external_id)
                self.state.record_event("takeover", self.name, dataset.name, dataset.external_id)
                if "ownership-drifted" not in dataset.tags:
                    dataset.tags.append("ownership-drifted")
            except Exception:
                self.state.record_event("takeover-failed", self.name, dataset.name, dataset.external_id)

        # -- 3. Permission sprawl — phantom users on workspaces ---------------
        for workspace in workspaces:
            if rng.random() >= scenarios.permission_sprawl_rate:
                continue
            role = rng.choice(_PBI_PHANTOM_ROLES)
            phantom = rng.choice(_PBI_PHANTOM_EMAILS)
            label = f"{workspace.name} += {phantom} ({role})"
            if dry_run:
                self.state.record_event("would-add-user", self.name, label, workspace.external_id)
                continue
            try:
                self._add_workspace_user(workspace.external_id, phantom, "User", role)
                self.state.record_event("add-user", self.name, label, workspace.external_id)
            except Exception:
                self.state.record_event("add-user-failed", self.name, label, workspace.external_id)

        # -- 4. Duplicate drift — clone reports with noisy names --------------
        for report in list(reports):
            if rng.random() >= scenarios.duplicate_drift_rate:
                continue
            suffix = rng.choice(_PBI_EVOLVE_SUFFIXES)
            clone_name = f"{report.name} {suffix}"
            if dry_run:
                self.state.record_event("would-clone", self.name, clone_name, report.external_id)
                continue
            try:
                new_id = self._clone_report(
                    report.parent_external_id or "", report.external_id, clone_name,
                )
                self.state.add_or_update(TrackedObject(
                    platform=self.name,
                    kind="report",
                    name=clone_name,
                    external_id=new_id,
                    parent_external_id=report.parent_external_id,
                    domain=report.domain,
                    team=report.team,
                    template_family=report.template_family,
                    source_ref=report.source_ref,
                    tags=sorted(set(report.tags + ["duplicate", "evolve-clone"])),
                ))
                self.state.record_event("clone", self.name, clone_name, new_id)
            except Exception:
                self.state.record_event("clone-failed", self.name, clone_name, report.external_id)

        # -- 5. Schedule chaos — weird refresh schedules ----------------------
        for dataset in datasets:
            if "stale" in dataset.tags:
                continue
            if rng.random() >= scenarios.schedule_chaos_rate:
                continue
            days = rng.sample(_PBI_SCHEDULE_DAYS, rng.randint(1, 7))
            times = rng.sample(_PBI_SCHEDULE_TIMES, rng.randint(1, 4))
            if dry_run:
                self.state.record_event(
                    "would-reschedule", self.name,
                    f"{dataset.name} -> {','.join(days)} @ {','.join(sorted(times))}",
                    dataset.external_id,
                )
                continue
            try:
                self._update_refresh_schedule(
                    dataset.parent_external_id or "", dataset.external_id, days, times,
                )
                self.state.record_event("reschedule", self.name, dataset.name, dataset.external_id)
                if "schedule-chaotic" not in dataset.tags:
                    dataset.tags.append("schedule-chaotic")
            except Exception:
                self.state.record_event("reschedule-failed", self.name, dataset.name, dataset.external_id)

        # -- 6. Connection drift — rebind reports to wrong datasets -----------
        if len(datasets) > 1:
            for report in reports:
                if rng.random() >= scenarios.connection_drift_rate:
                    continue
                wrong_dataset = rng.choice(datasets)
                if report.linked_to and wrong_dataset.external_id in report.linked_to:
                    continue
                label = f"{report.name} -> {wrong_dataset.name}"
                if dry_run:
                    self.state.record_event("would-rebind", self.name, label, report.external_id)
                    continue
                try:
                    self._rebind_report(
                        report.parent_external_id or "",
                        report.external_id,
                        wrong_dataset.external_id,
                    )
                    report.linked_to = [wrong_dataset.external_id]
                    if "connection-drifted" not in report.tags:
                        report.tags.append("connection-drifted")
                    self.state.record_event("rebind", self.name, label, report.external_id)
                except Exception:
                    self.state.record_event("rebind-failed", self.name, label, report.external_id)

        if not dry_run:
            self._save_checkpoint(state_path)

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

    def _rebind_report(self, workspace_id: str, report_id: str, dataset_id: str) -> None:
        request_json(
            "POST",
            f"{self.api_base}/groups/{workspace_id}/reports/{report_id}/Rebind",
            headers=self._headers(),
            body={"datasetId": dataset_id},
        )

    def _delete_report(self, workspace_id: str, report_id: str) -> None:
        if not report_id:
            return
        request_json(
            "DELETE",
            f"{self.api_base}/groups/{workspace_id}/reports/{report_id}",
            headers=self._headers(),
        )

    def _delete_dataset(self, workspace_id: str, dataset_id: str) -> None:
        if not dataset_id:
            return
        request_json(
            "DELETE",
            f"{self.api_base}/groups/{workspace_id}/datasets/{dataset_id}",
            headers=self._headers(),
        )

    def _find_base_dataset(self, base_name: str) -> TrackedObject | None:
        for obj in self.state.find(platform=self.name, kind="dataset"):
            if obj.name == base_name:
                return obj
        return None

    def _clone_report(self, workspace_id: str, report_id: str, new_name: str, target_workspace_id: str | None = None) -> str:
        body: dict[str, str] = {"name": new_name}
        if target_workspace_id:
            body["targetWorkspaceId"] = target_workspace_id
        response = request_json(
            "POST",
            f"{self.api_base}/groups/{workspace_id}/reports/{report_id}/Clone",
            headers=self._headers(),
            body=body,
        ).json()
        return str(response["id"])

    def _take_over_dataset(self, workspace_id: str, dataset_id: str) -> None:
        if not workspace_id:
            return
        request_json(
            "POST",
            f"{self.api_base}/groups/{workspace_id}/datasets/{dataset_id}/Default.TakeOver",
            headers=self._headers(),
        )

    def _add_workspace_user(self, workspace_id: str, identifier: str, principal_type: str, access_right: str) -> None:
        request_json(
            "POST",
            f"{self.api_base}/groups/{workspace_id}/users",
            headers=self._headers(),
            body={
                "identifier": identifier,
                "principalType": principal_type,
                "groupUserAccessRight": access_right,
            },
        )

    def _update_refresh_schedule(self, workspace_id: str, dataset_id: str, days: list[str], times: list[str]) -> None:
        if not workspace_id:
            return
        request_json(
            "PATCH",
            f"{self.api_base}/groups/{workspace_id}/datasets/{dataset_id}/refreshSchedule",
            headers=self._headers(),
            body={
                "value": {
                    "enabled": True,
                    "days": days,
                    "times": sorted(times),
                    "localTimeZoneId": "UTC",
                    "notifyOption": "NoNotification",
                },
            },
        )

    def _delete_workspace(self, workspace_id: str) -> None:
        request_json(
            "DELETE",
            f"{self.api_base}/groups/{workspace_id}",
            headers=self._headers(),
        )
