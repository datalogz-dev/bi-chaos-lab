import os
import tempfile
import textwrap
import unittest
from pathlib import Path

from bi_chaos_lab.manifest import Manifest


class ManifestTest(unittest.TestCase):
    def test_load_json_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "templates").mkdir()
            (root / "templates" / "sales.pbix").write_bytes(b"pbix")
            (root / "templates" / "sales.twbx").write_bytes(b"twbx")
            manifest = root / "manifest.json"
            manifest.write_text(
                textwrap.dedent(
                    """
                    {
                      "name": "sandbox",
                      "random_seed": 7,
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
                          "kind": "snowflake",
                          "owner": "data-platform",
                          "connection_hint": "snowflake://warehouse"
                        }
                      ],
                      "template_families": [
                        {
                          "name": "sales-pbi",
                          "platform": "powerbi",
                          "asset_kind": "report",
                          "path": "templates/sales.pbix",
                          "source_ref": "warehouse",
                          "owners": ["alice@example.com"],
                          "mutation_tags": ["executive"]
                        },
                        {
                          "name": "sales-tableau",
                          "platform": "tableau",
                          "asset_kind": "workbook",
                          "path": "templates/sales.twbx",
                          "source_ref": "warehouse"
                        }
                      ],
                      "domains": [
                        {
                          "name": "Sales",
                          "teams": ["Ops", "Leadership"],
                          "powerbi_workspaces_per_team": 1,
                          "tableau_projects_per_team": 1,
                          "asset_multiplier": 2,
                          "template_families": ["sales-pbi", "sales-tableau"]
                        }
                      ],
                      "scenarios": {
                        "refresh_failure_rate": 0.2
                      }
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
                loaded = Manifest.load(manifest)
            finally:
                for key, value in previous.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

            self.assertEqual(loaded.name, "sandbox")
            self.assertEqual(loaded.random_seed, 7)
            self.assertEqual(len(loaded.template_families), 2)


if __name__ == "__main__":
    unittest.main()
