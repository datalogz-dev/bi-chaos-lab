from __future__ import annotations

import argparse
import json
from pathlib import Path

from bi_chaos_lab.manifest import Manifest, ManifestError
from bi_chaos_lab.providers.powerbi import PowerBIProvider
from bi_chaos_lab.providers.tableau import TableauProvider
from bi_chaos_lab.scenario_engine import build_seed_plan
from bi_chaos_lab.state import StateFile


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Provision a sprawling Power BI and Tableau sandbox estate.")
    parser.add_argument("--manifest", required=True, help="Path to the TOML or JSON manifest")
    parser.add_argument("--state", default=".bi-chaos-lab-state.json", help="Path to the generated state file")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate", help="Validate manifest and credentials")
    validate.add_argument("--dry-run", action="store_true", help="Validate manifest only")

    seed = subparsers.add_parser("seed", help="Seed the initial estate")
    seed.add_argument("--dry-run", action="store_true", help="Plan only, do not call APIs")
    seed.add_argument("--show-plan", action="store_true", help="Print the generated seed plan")

    evolve = subparsers.add_parser("evolve", help="Apply drift and activity to an existing estate")
    evolve.add_argument("--dry-run", action="store_true", help="Plan only, do not call APIs")

    teardown = subparsers.add_parser("teardown", help="Delete all tracked sandbox assets")
    teardown.add_argument("--dry-run", action="store_true", help="Plan only, do not call APIs")
    return parser


def _providers(manifest: Manifest, state: StateFile):
    return [
        PowerBIProvider(manifest, state),
        TableauProvider(manifest, state),
    ]


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        manifest = Manifest.load(args.manifest)
    except ManifestError as exc:
        parser.error(str(exc))

    state = StateFile.load(args.state, manifest.name)
    providers = _providers(manifest, state)

    if args.command == "validate":
        if not args.dry_run:
            for provider in providers:
                provider.validate()
        print(json.dumps({"status": "ok", "manifest": manifest.name}, indent=2))
        return 0

    seed_plan = build_seed_plan(manifest)

    if args.command == "seed":
        if args.show_plan or args.dry_run:
            preview = {
                "powerbi_workspaces": seed_plan.powerbi_workspaces,
                "tableau_projects": seed_plan.tableau_projects,
                "assets": [
                    {
                        "platform": asset.platform,
                        "container_name": asset.container_name,
                        "asset_name": asset.asset_name,
                        "kind": asset.kind,
                        "template_family": asset.template_family,
                        "tags": asset.tags,
                    }
                    for asset in seed_plan.assets
                ],
            }
            print(json.dumps(preview, indent=2))
        for provider in providers:
            provider.seed(seed_plan, dry_run=args.dry_run, state_path=args.state)
        if not args.dry_run:
            state.save(args.state)
        return 0

    if args.command == "evolve":
        for provider in providers:
            provider.evolve(seed_plan.assets, dry_run=args.dry_run, state_path=args.state)
        if not args.dry_run:
            state.save(args.state)
        return 0

    if args.command == "teardown":
        for provider in providers:
            provider.teardown(dry_run=args.dry_run)
        if not args.dry_run:
            state.objects = []
            state.save(args.state)
        return 0

    parser.error(f"unsupported command: {args.command}")
    return 2
