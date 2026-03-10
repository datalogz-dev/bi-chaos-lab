import json
import tempfile
import unittest
from pathlib import Path

from bi_chaos_lab.state import StateFile, TrackedObject


class StateFileTest(unittest.TestCase):
    def test_save_and_load_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            state = StateFile(manifest_name="sandbox")
            state.add(
                TrackedObject(
                    platform="powerbi",
                    kind="workspace",
                    name="DZ-SBX Sales Ops",
                    external_id="123",
                )
            )
            state.save(state_path)
            payload = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["manifest_name"], "sandbox")
            loaded = StateFile.load(state_path, "sandbox")
            self.assertEqual(loaded.objects[0].external_id, "123")

    def test_container_aware_identity_keeps_same_name_in_different_parents(self) -> None:
        state = StateFile(manifest_name="sandbox")
        state.add_or_update(
            TrackedObject(
                platform="tableau",
                kind="workbook",
                name="Executive Overview",
                external_id="wb-1",
                parent_external_id="project-1",
            )
        )
        state.add_or_update(
            TrackedObject(
                platform="tableau",
                kind="workbook",
                name="Executive Overview",
                external_id="wb-2",
                parent_external_id="project-2",
            )
        )
        self.assertEqual(len(state.objects), 2)
        self.assertEqual(
            state.find_one(
                platform="tableau",
                kind="workbook",
                name="Executive Overview",
                parent_external_id="project-2",
            ).external_id,
            "wb-2",
        )


if __name__ == "__main__":
    unittest.main()
