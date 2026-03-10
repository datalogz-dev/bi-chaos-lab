from __future__ import annotations

import random
from dataclasses import dataclass, field

from bi_chaos_lab.manifest import DomainConfig, Manifest, TemplateFamily


@dataclass
class AssetPlan:
    platform: str
    container_name: str
    container_parent: str | None
    asset_name: str
    kind: str
    domain: str
    team: str
    template_family: str
    source_ref: str
    tags: list[str] = field(default_factory=list)


@dataclass
class SeedPlan:
    powerbi_workspaces: list[dict[str, str]]
    tableau_projects: list[dict[str, str]]
    assets: list[AssetPlan]


_TEAM_SUFFIXES = [
    "North America",
    "EMEA",
    "LATAM",
    "Ops",
    "Self-Service",
    "Leadership",
    "RevOps",
    "Forecasting",
]

_EXECUTIVE_TITLES = ["Executive", "Board", "SteerCo", "QBR", "All Hands"]
_ASSET_TITLES = ["Pulse", "Performance", "Pipeline", "Utilization", "Variance", "Health", "Insights", "Review"]
_NOISE_SUFFIXES = ["v2", "FINAL", "Draft", "Legacy", "Needs Cleanup", "Temp", "Copy", "Promoted"]


def _pick_tags(rng: random.Random, domain: DomainConfig, base_tags: list[str]) -> list[str]:
    tags = list(base_tags)
    if rng.random() < domain.executive_ratio:
        tags.append("executive")
    if rng.random() < domain.shadow_ratio:
        tags.append("shadow")
    if rng.random() < domain.stale_ratio:
        tags.append("stale")
    if rng.random() < domain.duplicate_ratio:
        tags.append("duplicate-prone")
    if rng.random() < 0.15:
        tags.append("high-visibility")
    return sorted(set(tags))


def _choose_title(rng: random.Random, domain_name: str, team: str, tags: list[str]) -> str:
    parts = [domain_name, team, rng.choice(_ASSET_TITLES)]
    if "executive" in tags:
        parts.append(rng.choice(_EXECUTIVE_TITLES))
    if "shadow" in tags or rng.random() < 0.2:
        parts.append(rng.choice(_NOISE_SUFFIXES))
    return " ".join(parts)


def _families_for_platform(families: list[TemplateFamily], platform: str, names: set[str]) -> list[TemplateFamily]:
    return [family for family in families if family.platform == platform and family.name in names]


def _append_unique_asset(assets: list[AssetPlan], candidate: AssetPlan, rng: random.Random) -> None:
    taken = {(asset.platform, asset.container_name, asset.asset_name) for asset in assets}
    while (candidate.platform, candidate.container_name, candidate.asset_name) in taken:
        candidate.asset_name = f"{candidate.asset_name} {rng.choice(_NOISE_SUFFIXES)}"
    assets.append(candidate)


def build_seed_plan(manifest: Manifest) -> SeedPlan:
    rng = random.Random(manifest.random_seed)
    powerbi_workspaces: list[dict[str, str]] = []
    tableau_projects: list[dict[str, str]] = []
    assets: list[AssetPlan] = []

    family_map = {family.name: family for family in manifest.template_families}

    for domain in manifest.domains:
        domain_family_names = set(domain.template_families)
        pb_families = _families_for_platform(manifest.template_families, "powerbi", domain_family_names)
        tb_families = _families_for_platform(manifest.template_families, "tableau", domain_family_names)

        for team_index, team in enumerate(domain.teams):
            pb_names: list[str] = []
            for idx in range(domain.powerbi_workspaces_per_team):
                suffix = _TEAM_SUFFIXES[(team_index + idx) % len(_TEAM_SUFFIXES)]
                name = f"{manifest.safety.workspace_prefix} {domain.name} {team} {suffix}"
                powerbi_workspaces.append({"name": name, "domain": domain.name, "team": team})
                pb_names.append(name)

            parent_name = f"{manifest.safety.project_prefix} {domain.name}"
            if not any(project["name"] == parent_name for project in tableau_projects):
                tableau_projects.append({"name": parent_name, "parent": "", "domain": domain.name, "team": ""})
            tb_names: list[str] = []
            for idx in range(domain.tableau_projects_per_team):
                child_name = f"{parent_name} / {team} {_TEAM_SUFFIXES[(team_index + idx) % len(_TEAM_SUFFIXES)]}"
                tableau_projects.append({"name": child_name, "parent": parent_name, "domain": domain.name, "team": team})
                tb_names.append(child_name)

            for family in pb_families:
                for _ in range(domain.asset_multiplier):
                    tags = _pick_tags(rng, domain, family.mutation_tags)
                    workspace_name = rng.choice(pb_names)
                    _append_unique_asset(
                        assets,
                        AssetPlan(
                            platform="powerbi",
                            container_name=workspace_name,
                            container_parent=None,
                            asset_name=_choose_title(rng, domain.name, team, tags),
                            kind=family.asset_kind,
                            domain=domain.name,
                            team=team,
                            template_family=family.name,
                            source_ref=family.source_ref,
                            tags=tags,
                        ),
                        rng,
                    )

            for family in tb_families:
                for _ in range(domain.asset_multiplier):
                    tags = _pick_tags(rng, domain, family.mutation_tags)
                    project_name = rng.choice(tb_names)
                    _append_unique_asset(
                        assets,
                        AssetPlan(
                            platform="tableau",
                            container_name=project_name,
                            container_parent=parent_name,
                            asset_name=_choose_title(rng, domain.name, team, tags),
                            kind=family.asset_kind,
                            domain=domain.name,
                            team=team,
                            template_family=family.name,
                            source_ref=family.source_ref,
                            tags=tags,
                        ),
                        rng,
                    )

    for plan in list(assets):
        if "duplicate-prone" in plan.tags and rng.random() < 0.5:
            family = family_map[plan.template_family]
            duplicate_name = f"{plan.asset_name} {rng.choice(_NOISE_SUFFIXES)}"
            _append_unique_asset(
                assets,
                AssetPlan(
                    platform=plan.platform,
                    container_name=plan.container_name,
                    container_parent=plan.container_parent,
                    asset_name=duplicate_name,
                    kind=plan.kind,
                    domain=plan.domain,
                    team=plan.team,
                    template_family=family.name,
                    source_ref=family.source_ref,
                    tags=sorted(set(plan.tags + ["duplicate"])),
                ),
                rng,
            )

    return SeedPlan(
        powerbi_workspaces=powerbi_workspaces,
        tableau_projects=tableau_projects,
        assets=assets,
    )
