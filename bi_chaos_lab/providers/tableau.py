from __future__ import annotations

import os
import mimetypes
import uuid
import xml.etree.ElementTree as ET
from xml.sax.saxutils import quoteattr
from dataclasses import dataclass

from bi_chaos_lab.http import HTTPError, request_bytes
from bi_chaos_lab.manifest import Manifest
from bi_chaos_lab.providers.base import Provider
from bi_chaos_lab.scenario_engine import AssetPlan, SeedPlan
from bi_chaos_lab.state import StateFile, TrackedObject


@dataclass
class _Auth:
    token: str
    site_id: str


class TableauProvider(Provider):
    name = "tableau"

    def __init__(self, manifest: Manifest, state: StateFile) -> None:
        super().__init__(manifest, state)
        self._auth: _Auth | None = None

    @property
    def enabled(self) -> bool:
        return self.manifest.platforms.tableau.enabled

    def validate(self) -> None:
        if not self.enabled:
            return
        self._signin(force=True)

    def seed(self, plan: SeedPlan, *, dry_run: bool) -> None:
        if not self.enabled:
            return
        if not dry_run:
            self._reconcile_existing_state(plan)
        for project in plan.tableau_projects:
            existing = self._find_project(project["name"])
            if existing:
                continue
            if dry_run:
                self.state.record_event("would-create", self.name, project["name"], "dry-run")
                continue
            parent_id = None
            if project["parent"]:
                parent = self._find_project(project["parent"])
                if parent is None:
                    raise RuntimeError(f"parent project was not created: {project['parent']}")
                parent_id = parent.external_id
            project_id = self._create_project(project["name"], parent_id)
            self.state.add_or_update(
                TrackedObject(
                    platform=self.name,
                    kind="project",
                    name=project["name"],
                    external_id=project_id,
                    parent_external_id=parent_id,
                    domain=project["domain"],
                    team=project["team"] or None,
                )
            )
            self.state.record_event("create", self.name, project["name"], project_id)

        family_map = {family.name: family for family in self.manifest.template_families if family.platform == self.name}
        for asset in [item for item in plan.assets if item.platform == self.name]:
            if dry_run:
                self.state.record_event("would-publish", self.name, asset.asset_name, "dry-run")
                continue
            project = self._find_project(asset.container_name)
            if project is None:
                raise RuntimeError(f"project was not created: {asset.container_name}")
            if self._find_tracked(asset.kind, asset.asset_name, project.external_id):
                continue
            family = family_map[asset.template_family]
            asset_id = self._publish_asset(
                project.external_id,
                asset.asset_name,
                family.asset_kind,
                str(self.template_path(family.path)),
            )
            self.state.add_or_update(
                TrackedObject(
                    platform=self.name,
                    kind=asset.kind,
                    name=asset.asset_name,
                    external_id=asset_id,
                    parent_external_id=project.external_id,
                    domain=asset.domain,
                    team=asset.team,
                    template_family=asset.template_family,
                    source_ref=asset.source_ref,
                    tags=list(asset.tags),
                )
            )
            self.state.record_event("create", self.name, asset.asset_name, asset_id)

    def evolve(self, plan: list[AssetPlan], *, dry_run: bool) -> None:
        if not self.enabled:
            return
        workbooks = self.state.find(platform=self.name, kind="workbook")
        for index, workbook in enumerate(workbooks):
            if "stale" in workbook.tags or index % 4 != 0:
                continue
            noisy_name = f"{workbook.name} Review"
            if dry_run:
                self.state.record_event("would-rename", self.name, noisy_name, workbook.external_id)
                continue
            self._rename_workbook(workbook.external_id, noisy_name)
            workbook.name = noisy_name
            self.state.record_event("rename", self.name, noisy_name, workbook.external_id)

    def teardown(self, *, dry_run: bool) -> None:
        if not self.enabled:
            return
        projects = list(reversed(self.state.find(platform=self.name, kind="project")))
        for project in projects:
            if self.manifest.safety.teardown_requires_prefix_match and not project.name.startswith(
                self.manifest.safety.project_prefix
            ):
                raise RuntimeError(f"refusing to delete project outside prefix: {project.name}")
            if dry_run:
                self.state.record_event("would-delete", self.name, project.name, project.external_id)
                continue
            self._delete_project(project.external_id)
            self.state.record_event("delete", self.name, project.name, project.external_id)

    def _base_url(self) -> str:
        cfg = self.manifest.platforms.tableau
        return f"https://{cfg.host_name()}/api/{cfg.api_version}"

    def _headers(self) -> dict[str, str]:
        auth = self._signin(force=False)
        return {
            "X-Tableau-Auth": auth.token,
        }

    def _site_url(self) -> str:
        auth = self._signin(force=False)
        return f"{self._base_url()}/sites/{auth.site_id}"

    def _request_with_reauth(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        body: bytes | None = None,
        timeout: int = 60,
    ):
        try:
            return request_bytes(method, url, headers=headers, body=body, timeout=timeout)
        except HTTPError as exc:
            if " failed with 401:" not in str(exc):
                raise
            self._auth = None
            refreshed_headers = dict(headers)
            refreshed_headers["X-Tableau-Auth"] = self._signin(force=True).token
            return request_bytes(method, url, headers=refreshed_headers, body=body, timeout=timeout)

    def _signin(self, *, force: bool) -> _Auth:
        if not force and self._auth is not None:
            return self._auth
        cfg = self.manifest.platforms.tableau
        payload = (
            "<tsRequest>"
            "<credentials personalAccessTokenName=\"{name}\" personalAccessTokenSecret=\"{secret}\">"
            "<site contentUrl=\"{site}\"/>"
            "</credentials>"
            "</tsRequest>"
        ).format(name=cfg.token_name(), secret=cfg.token_secret(), site=cfg.site_name)
        response = request_bytes(
            "POST",
            f"{self._base_url()}/auth/signin",
            headers={"Content-Type": "application/xml", "Accept": "application/xml"},
            body=payload.encode("utf-8"),
        )
        root = ET.fromstring(response.body)
        credentials = root.find(".//{*}credentials")
        site = root.find(".//{*}site")
        if credentials is None or site is None:
            raise RuntimeError("unexpected Tableau sign-in response")
        auth = _Auth(token=str(credentials.attrib["token"]), site_id=str(site.attrib["id"]))
        self._auth = auth
        return auth

    def _create_project(self, name: str, parent_id: str | None) -> str:
        attrs = [
            f"name={quoteattr(name)}",
            f"description={quoteattr('Managed by bi-chaos-lab')}",
        ]
        if parent_id:
            attrs.append(f"parentProjectId={quoteattr(parent_id)}")
        payload = f"<tsRequest><project {' '.join(attrs)}/></tsRequest>"
        response = self._request_with_reauth(
            "POST",
            f"{self._site_url()}/projects",
            headers={
                **self._headers(),
                "Content-Type": "application/xml",
                "Accept": "application/xml",
            },
            body=payload.encode("utf-8"),
        )
        root = ET.fromstring(response.body)
        project = root.find(".//{*}project")
        if project is None:
            raise RuntimeError("unexpected Tableau project create response")
        return str(project.attrib["id"])

    def _publish_asset(self, project_id: str, name: str, asset_kind: str, path: str) -> str:
        endpoint = "workbooks" if asset_kind in {"workbook", "view"} else "datasources"
        mime_type = mimetypes.guess_type(path)[0] or "application/octet-stream"
        boundary = f"----bi-chaos-lab-{uuid.uuid4().hex}"
        part_name = "tableau_workbook" if endpoint == "workbooks" else "tableau_datasource"
        xml_part = (
            "<tsRequest>"
            f"<{endpoint[:-1]} name={quoteattr(name)} showTabs=\"false\">"
            f"<project id={quoteattr(project_id)}/>"
            f"</{endpoint[:-1]}>"
            "</tsRequest>"
        )
        file_content = open(path, "rb").read()
        filename = os.path.basename(path)
        body = bytearray()
        for part_headers, part_body in (
            (
                {
                    "Content-Disposition": 'name="request_payload"',
                    "Content-Type": "text/xml",
                },
                xml_part.encode("utf-8"),
            ),
            (
                {
                    "Content-Disposition": f'name="{part_name}"; filename="{filename}"',
                    "Content-Type": "application/octet-stream" if endpoint == "workbooks" else mime_type,
                },
                file_content,
            ),
        ):
            body.extend(f"--{boundary}\r\n".encode("utf-8"))
            for key, value in part_headers.items():
                body.extend(f"{key}: {value}\r\n".encode("utf-8"))
            body.extend(b"\r\n")
            body.extend(part_body)
            body.extend(b"\r\n")
        body.extend(f"--{boundary}--\r\n".encode("utf-8"))
        response = self._request_with_reauth(
            "POST",
            f"{self._site_url()}/{endpoint}?overwrite=true",
            headers={
                **self._headers(),
                "Content-Type": f"multipart/mixed; boundary={boundary}",
                "Accept": "application/xml",
            },
            body=bytes(body),
            timeout=300,
        )
        root = ET.fromstring(response.body)
        element = root.find(f".//{{*}}{endpoint[:-1]}")
        if element is None:
            raise RuntimeError(f"unexpected Tableau publish response for {endpoint}")
        return str(element.attrib["id"])

    def _rename_workbook(self, workbook_id: str, name: str) -> None:
        payload = f"<tsRequest><workbook name={quoteattr(name)}/></tsRequest>"
        self._request_with_reauth(
            "PUT",
            f"{self._site_url()}/workbooks/{workbook_id}",
            headers={
                **self._headers(),
                "Content-Type": "application/xml",
                "Accept": "application/xml",
            },
            body=payload.encode("utf-8"),
        )

    def _delete_project(self, project_id: str) -> None:
        self._request_with_reauth(
            "DELETE",
            f"{self._site_url()}/projects/{project_id}",
            headers=self._headers(),
        )

    def _find_tracked(self, kind: str, name: str, parent_external_id: str | None = None) -> TrackedObject | None:
        return self.state.find_one(
            platform=self.name,
            kind=kind,
            name=name,
            parent_external_id=parent_external_id,
        )

    def _find_project(self, name: str) -> TrackedObject | None:
        for item in self.state.find(platform=self.name, kind="project"):
            if item.name == name:
                return item
        return None

    def _reconcile_existing_state(self, plan: SeedPlan) -> None:
        project_map = self._list_projects()
        for project in plan.tableau_projects:
            existing = project_map.get(project["name"])
            if not existing:
                continue
            self.state.add_or_update(
                TrackedObject(
                    platform=self.name,
                    kind="project",
                    name=project["name"],
                    external_id=existing["id"],
                    parent_external_id=existing.get("parentProjectId"),
                    domain=project["domain"],
                    team=project["team"] or None,
                )
            )

        workbooks = self._list_workbooks()
        for asset in [item for item in plan.assets if item.platform == self.name]:
            project = self._find_project(asset.container_name)
            if project is None:
                continue
            for workbook in workbooks:
                if workbook.get("name") == asset.asset_name and workbook.get("project_id") == project.external_id:
                    self.state.add_or_update(
                        TrackedObject(
                            platform=self.name,
                            kind="workbook",
                            name=asset.asset_name,
                            external_id=workbook["id"],
                            parent_external_id=project.external_id,
                            domain=asset.domain,
                            team=asset.team,
                            template_family=asset.template_family,
                            source_ref=asset.source_ref,
                            tags=list(asset.tags),
                        )
                    )

    def _list_projects(self) -> dict[str, dict[str, str | None]]:
        response = self._request_with_reauth(
            "GET",
            f"{self._site_url()}/projects?pageSize=1000&pageNumber=1",
            headers={
                **self._headers(),
                "Accept": "application/xml",
            },
        )
        root = ET.fromstring(response.body)
        return {
            str(node.attrib.get("name")): {
                "id": str(node.attrib.get("id")),
                "parentProjectId": node.attrib.get("parentProjectId"),
            }
            for node in root.findall(".//{*}project")
            if node.attrib.get("name")
        }

    def _list_workbooks(self) -> list[dict[str, str | None]]:
        response = self._request_with_reauth(
            "GET",
            f"{self._site_url()}/workbooks?pageSize=1000&pageNumber=1",
            headers={
                **self._headers(),
                "Accept": "application/xml",
            },
        )
        root = ET.fromstring(response.body)
        workbooks: list[dict[str, str | None]] = []
        for node in root.findall(".//{*}workbook"):
            project = node.find(".//{*}project")
            workbooks.append(
                {
                    "id": str(node.attrib.get("id")),
                    "name": node.attrib.get("name"),
                    "project_id": project.attrib.get("id") if project is not None else None,
                }
            )
        return workbooks
