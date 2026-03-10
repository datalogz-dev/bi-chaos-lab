from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11
    tomllib = None  # type: ignore[assignment]


class ManifestError(ValueError):
    pass


def _require_dict(data: Any, label: str) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise ManifestError(f"{label} must be an object")
    return data


def _require_list(data: Any, label: str) -> list[Any]:
    if not isinstance(data, list):
        raise ManifestError(f"{label} must be a list")
    return data


def _read_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ManifestError(f"manifest file not found: {path}")
    raw = path.read_bytes()
    if path.suffix == ".json":
        return _require_dict(json.loads(raw), "manifest")
    if path.suffix in {".toml", ".tml"}:
        if tomllib is None:
            raise ManifestError("TOML manifests require Python 3.11+ or an installed tomllib-compatible parser")
        return _require_dict(tomllib.loads(raw.decode("utf-8")), "manifest")
    raise ManifestError("manifest must use .json or .toml")


def _resolve_env(name: str | None, allow_empty: bool = False) -> str | None:
    if not name:
        return None
    value = os.getenv(name)
    if value is None:
        if allow_empty:
            return None
        raise ManifestError(f"environment variable is not set: {name}")
    if not value and not allow_empty:
        raise ManifestError(f"environment variable is empty: {name}")
    return value


@dataclass
class SafetyConfig:
    workspace_prefix: str
    project_prefix: str
    teardown_requires_prefix_match: bool = True
    allow_destructive_without_state: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SafetyConfig":
        return cls(
            workspace_prefix=str(data.get("workspace_prefix", "")).strip(),
            project_prefix=str(data.get("project_prefix", "")).strip(),
            teardown_requires_prefix_match=bool(data.get("teardown_requires_prefix_match", True)),
            allow_destructive_without_state=bool(data.get("allow_destructive_without_state", False)),
        )

    def validate(self) -> None:
        if not self.workspace_prefix:
            raise ManifestError("safety.workspace_prefix is required")
        if not self.project_prefix:
            raise ManifestError("safety.project_prefix is required")


@dataclass
class PlatformPowerBI:
    enabled: bool
    tenant_id_env: str | None
    client_id_env: str | None
    client_secret_env: str | None
    workspace_prefix: str
    root_capacity: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PlatformPowerBI":
        return cls(
            enabled=bool(data.get("enabled", False)),
            tenant_id_env=str(data.get("tenant_id_env", "")).strip() or None,
            client_id_env=str(data.get("client_id_env", "")).strip() or None,
            client_secret_env=str(data.get("client_secret_env", "")).strip() or None,
            workspace_prefix=str(data.get("workspace_prefix", "")).strip(),
            root_capacity=str(data.get("root_capacity", "")).strip() or None,
        )

    def validate(self) -> None:
        if not self.enabled:
            return
        missing = [
            name
            for name, value in (
                ("tenant_id_env", self.tenant_id_env),
                ("client_id_env", self.client_id_env),
                ("client_secret_env", self.client_secret_env),
            )
            if not value
        ]
        if missing:
            raise ManifestError(f"powerbi is enabled but missing credentials: {', '.join(missing)}")
        if not self.workspace_prefix:
            raise ManifestError("platforms.powerbi.workspace_prefix is required")

    def tenant_id(self) -> str:
        return _resolve_env(self.tenant_id_env) or ""

    def client_id(self) -> str:
        return _resolve_env(self.client_id_env) or ""

    def client_secret(self) -> str:
        return _resolve_env(self.client_secret_env) or ""


@dataclass
class PlatformTableau:
    enabled: bool
    host_name_env: str | None
    site_name: str
    api_version: str
    token_name_env: str | None
    token_secret_env: str | None
    project_prefix: str

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PlatformTableau":
        return cls(
            enabled=bool(data.get("enabled", False)),
            host_name_env=str(data.get("host_name_env", "")).strip() or None,
            site_name=str(data.get("site_name", "")).strip(),
            api_version=str(data.get("api_version", "3.25")).strip(),
            token_name_env=str(data.get("token_name_env", "")).strip() or None,
            token_secret_env=str(data.get("token_secret_env", "")).strip() or None,
            project_prefix=str(data.get("project_prefix", "")).strip(),
        )

    def validate(self) -> None:
        if not self.enabled:
            return
        missing = [
            name
            for name, value in (
                ("host_name_env", self.host_name_env),
                ("token_name_env", self.token_name_env),
                ("token_secret_env", self.token_secret_env),
            )
            if not value
        ]
        if missing:
            raise ManifestError(f"tableau is enabled but missing credentials: {', '.join(missing)}")
        if not self.site_name:
            raise ManifestError("platforms.tableau.site_name is required")
        if not self.project_prefix:
            raise ManifestError("platforms.tableau.project_prefix is required")

    def host_name(self) -> str:
        return _resolve_env(self.host_name_env) or ""

    def token_name(self) -> str:
        return _resolve_env(self.token_name_env) or ""

    def token_secret(self) -> str:
        return _resolve_env(self.token_secret_env) or ""


@dataclass
class PlatformsConfig:
    powerbi: PlatformPowerBI
    tableau: PlatformTableau

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PlatformsConfig":
        data = _require_dict(data, "platforms")
        return cls(
            powerbi=PlatformPowerBI.from_dict(_require_dict(data.get("powerbi", {}), "platforms.powerbi")),
            tableau=PlatformTableau.from_dict(_require_dict(data.get("tableau", {}), "platforms.tableau")),
        )

    def validate(self) -> None:
        self.powerbi.validate()
        self.tableau.validate()
        if not self.powerbi.enabled and not self.tableau.enabled:
            raise ManifestError("at least one platform must be enabled")


@dataclass
class SourceConfig:
    name: str
    kind: str
    owner: str
    connection_hint: str

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SourceConfig":
        return cls(
            name=str(data.get("name", "")).strip(),
            kind=str(data.get("kind", "")).strip(),
            owner=str(data.get("owner", "data-platform")).strip(),
            connection_hint=str(data.get("connection_hint", "")).strip(),
        )

    def validate(self) -> None:
        if not self.name:
            raise ManifestError("source.name is required")
        if not self.kind:
            raise ManifestError(f"source {self.name!r} is missing kind")


@dataclass
class TemplateFamily:
    name: str
    platform: str
    asset_kind: str
    path: str
    source_ref: str
    owners: list[str] = field(default_factory=list)
    mutation_tags: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TemplateFamily":
        return cls(
            name=str(data.get("name", "")).strip(),
            platform=str(data.get("platform", "")).strip().lower(),
            asset_kind=str(data.get("asset_kind", "")).strip().lower(),
            path=str(data.get("path", "")).strip(),
            source_ref=str(data.get("source_ref", "")).strip(),
            owners=[str(value).strip() for value in _require_list(data.get("owners", []), "template_family.owners")],
            mutation_tags=[
                str(value).strip() for value in _require_list(data.get("mutation_tags", []), "template_family.mutation_tags")
            ],
        )

    def validate(self, root: Path, source_names: set[str]) -> None:
        if not self.name:
            raise ManifestError("template_family.name is required")
        if self.platform not in {"powerbi", "tableau"}:
            raise ManifestError(f"template_family {self.name!r} has invalid platform")
        if self.asset_kind not in {"dataset", "report", "dashboard", "workbook", "datasource", "view"}:
            raise ManifestError(f"template_family {self.name!r} has invalid asset_kind")
        if self.source_ref not in source_names:
            raise ManifestError(f"template_family {self.name!r} references unknown source_ref {self.source_ref!r}")
        resolved = (root / self.path).resolve()
        if not resolved.exists():
            raise ManifestError(f"template_family {self.name!r} path not found: {resolved}")


@dataclass
class DomainConfig:
    name: str
    teams: list[str]
    powerbi_workspaces_per_team: int
    tableau_projects_per_team: int
    asset_multiplier: int
    template_families: list[str]
    executive_ratio: float = 0.1
    shadow_ratio: float = 0.2
    stale_ratio: float = 0.25
    duplicate_ratio: float = 0.3

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DomainConfig":
        return cls(
            name=str(data.get("name", "")).strip(),
            teams=[str(value).strip() for value in _require_list(data.get("teams", []), "domain.teams")],
            powerbi_workspaces_per_team=int(data.get("powerbi_workspaces_per_team", 2)),
            tableau_projects_per_team=int(data.get("tableau_projects_per_team", 2)),
            asset_multiplier=int(data.get("asset_multiplier", 3)),
            template_families=[
                str(value).strip() for value in _require_list(data.get("template_families", []), "domain.template_families")
            ],
            executive_ratio=float(data.get("executive_ratio", 0.1)),
            shadow_ratio=float(data.get("shadow_ratio", 0.2)),
            stale_ratio=float(data.get("stale_ratio", 0.25)),
            duplicate_ratio=float(data.get("duplicate_ratio", 0.3)),
        )

    def validate(self, family_names: set[str]) -> None:
        if not self.name:
            raise ManifestError("domain.name is required")
        if not self.teams:
            raise ManifestError(f"domain {self.name!r} must define at least one team")
        if self.asset_multiplier < 1:
            raise ManifestError(f"domain {self.name!r} asset_multiplier must be >= 1")
        for family_name in self.template_families:
            if family_name not in family_names:
                raise ManifestError(f"domain {self.name!r} references unknown template family {family_name!r}")


@dataclass
class ScenarioConfig:
    refresh_failure_rate: float = 0.12
    ownership_drift_rate: float = 0.18
    export_spike_rate: float = 0.08
    dormant_ratio: float = 0.3
    rename_noise_rate: float = 0.15

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ScenarioConfig":
        return cls(
            refresh_failure_rate=float(data.get("refresh_failure_rate", 0.12)),
            ownership_drift_rate=float(data.get("ownership_drift_rate", 0.18)),
            export_spike_rate=float(data.get("export_spike_rate", 0.08)),
            dormant_ratio=float(data.get("dormant_ratio", 0.3)),
            rename_noise_rate=float(data.get("rename_noise_rate", 0.15)),
        )


@dataclass
class Manifest:
    path: Path
    name: str
    random_seed: int
    safety: SafetyConfig
    platforms: PlatformsConfig
    sources: list[SourceConfig]
    template_families: list[TemplateFamily]
    domains: list[DomainConfig]
    scenarios: ScenarioConfig

    @property
    def root(self) -> Path:
        return self.path.parent

    @classmethod
    def load(cls, path: str | Path) -> "Manifest":
        manifest_path = Path(path).expanduser().resolve()
        data = _read_manifest(manifest_path)
        manifest = cls(
            path=manifest_path,
            name=str(data.get("name", "")).strip(),
            random_seed=int(data.get("random_seed", 42)),
            safety=SafetyConfig.from_dict(_require_dict(data.get("safety", {}), "safety")),
            platforms=PlatformsConfig.from_dict(data.get("platforms", {})),
            sources=[SourceConfig.from_dict(_require_dict(item, "source")) for item in _require_list(data.get("sources", []), "sources")],
            template_families=[
                TemplateFamily.from_dict(_require_dict(item, "template_family"))
                for item in _require_list(data.get("template_families", []), "template_families")
            ],
            domains=[DomainConfig.from_dict(_require_dict(item, "domain")) for item in _require_list(data.get("domains", []), "domains")],
            scenarios=ScenarioConfig.from_dict(_require_dict(data.get("scenarios", {}), "scenarios")),
        )
        manifest.validate()
        return manifest

    def validate(self) -> None:
        if not self.name:
            raise ManifestError("manifest name is required")
        self.safety.validate()
        self.platforms.validate()
        if not self.sources:
            raise ManifestError("manifest must define at least one source")
        source_names = {source.name for source in self.sources}
        for source in self.sources:
            source.validate()
        if not self.template_families:
            raise ManifestError("manifest must define at least one template family")
        family_names = {family.name for family in self.template_families}
        for family in self.template_families:
            family.validate(self.root, source_names)
        if not self.domains:
            raise ManifestError("manifest must define at least one domain")
        for domain in self.domains:
            domain.validate(family_names)
