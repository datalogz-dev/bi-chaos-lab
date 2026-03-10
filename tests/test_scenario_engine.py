import os
import tempfile
import textwrap
import unittest
from pathlib import Path

from bi_chaos_lab.manifest import Manifest
from bi_chaos_lab.scenario_engine import build_seed_plan


class ScenarioEngineTest(unittest.TestCase):
    def test_seed_plan_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "templates").mkdir()
            (root / "templates" / "pbix.pbix").write_bytes(b"pbix")
            (root / "templates" / "tb.twbx").write_bytes(b"twbx")
            manifest_path = root / "manifest.json"
            manifest_path.write_text(
                textwrap.dedent(
                    """
                    {
                      "name": "sandbox",
                      "random_seed": 5,
                      "safety": {
                        "workspace_prefix": "DZ-SBX",
                        "project_prefix": "DZ-SBX"
                      },
                      "platforms": {
                        "powerbi": {
                          "enabled": true,
                          "tenant_id_env": "TEST_PBI_TENANT",
                          "client_id_env": "TEST_PBI_CLIENT",
                          "client_secret_env": "TEST_PBI_SECRET",
                          "workspace_prefix": "DZ-SBX"
                        },
                        "tableau": {
                          "enabled": true,
                          "host_name_env": "TEST_TABLEAU_HOST",
                          "token_name_env": "TEST_TABLEAU_TOKEN_NAME",
                          "token_secret_env": "TEST_TABLEAU_TOKEN_SECRET",
                          "site_name": "sandbox",
                          "project_prefix": "DZ-SBX"
                        }
                      },
                      "sources": [
                        {
                          "name": "warehouse",
                          "kind": "postgres",
                          "owner": "analytics",
                          "connection_hint": "postgres://warehouse"
                        }
                      ],
                      "template_families": [
                        {
                          "name": "pbi",
                          "platform": "powerbi",
                          "asset_kind": "report",
                          "path": "templates/pbix.pbix",
                          "source_ref": "warehouse"
                        },
                        {
                          "name": "tableau",
                          "platform": "tableau",
                          "asset_kind": "workbook",
                          "path": "templates/tb.twbx",
                          "source_ref": "warehouse"
                        }
                      ],
                      "domains": [
                        {
                          "name": "Finance",
                          "teams": ["Core"],
                          "powerbi_workspaces_per_team": 2,
                          "tableau_projects_per_team": 2,
                          "asset_multiplier": 2,
                          "template_families": ["pbi", "tableau"]
                        }
                      ]
                    }
                    """
                ).strip(),
                encoding="utf-8",
            )

            env = {
                "TEST_PBI_TENANT": "tenant",
                "TEST_PBI_CLIENT": "client",
                "TEST_PBI_SECRET": "secret",
                "TEST_TABLEAU_HOST": "tableau.example.com",
                "TEST_TABLEAU_TOKEN_NAME": "token-name",
                "TEST_TABLEAU_TOKEN_SECRET": "token-secret",
            }
            previous = {key: os.environ.get(key) for key in env}
            os.environ.update(env)
            try:
                manifest = Manifest.load(manifest_path)
            finally:
                for key, value in previous.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

            plan_a = build_seed_plan(manifest)
            plan_b = build_seed_plan(manifest)
            self.assertEqual(plan_a.powerbi_workspaces, plan_b.powerbi_workspaces)
            self.assertEqual(plan_a.tableau_projects, plan_b.tableau_projects)
            self.assertEqual(
                [(asset.platform, asset.container_name, asset.asset_name) for asset in plan_a.assets],
                [(asset.platform, asset.container_name, asset.asset_name) for asset in plan_b.assets],
            )
            unique_keys = {(asset.platform, asset.container_name, asset.asset_name) for asset in plan_a.assets}
            self.assertEqual(len(unique_keys), len(plan_a.assets))


if __name__ == "__main__":
    unittest.main()
