from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path


@dataclass
class TrackedObject:
    platform: str
    kind: str
    name: str
    external_id: str
    parent_external_id: str | None = None
    domain: str | None = None
    team: str | None = None
    template_family: str | None = None
    source_ref: str | None = None
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass
class StateFile:
    manifest_name: str
    state_version: int = 1
    objects: list[TrackedObject] = field(default_factory=list)
    events: list[dict[str, str]] = field(default_factory=list)

    @classmethod
    def load(cls, path: str | Path, manifest_name: str) -> "StateFile":
        state_path = Path(path).expanduser().resolve()
        if not state_path.exists():
            return cls(manifest_name=manifest_name)
        data = json.loads(state_path.read_text(encoding="utf-8"))
        objects = [TrackedObject(**item) for item in data.get("objects", [])]
        return cls(
            manifest_name=data.get("manifest_name", manifest_name),
            state_version=int(data.get("state_version", 1)),
            objects=objects,
            events=list(data.get("events", [])),
        )

    def save(self, path: str | Path) -> None:
        state_path = Path(path).expanduser().resolve()
        state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = asdict(self)
        state_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def add(self, tracked: TrackedObject) -> None:
        self.objects.append(tracked)

    def add_or_update(self, tracked: TrackedObject) -> None:
        for index, existing in enumerate(self.objects):
            same_external = (
                existing.platform == tracked.platform
                and existing.kind == tracked.kind
                and existing.external_id == tracked.external_id
            )
            same_identity = (
                existing.platform == tracked.platform
                and existing.kind == tracked.kind
                and existing.name == tracked.name
                and existing.parent_external_id == tracked.parent_external_id
            )
            if same_external or same_identity:
                self.objects[index] = tracked
                return
        self.objects.append(tracked)

    def record_event(self, action: str, platform: str, name: str, external_id: str) -> None:
        self.events.append(
            {
                "action": action,
                "platform": platform,
                "name": name,
                "external_id": external_id,
            }
        )

    def find(self, *, platform: str, kind: str | None = None) -> list[TrackedObject]:
        return [
            item
            for item in self.objects
            if item.platform == platform and (kind is None or item.kind == kind)
        ]

    def find_one(
        self,
        *,
        platform: str,
        kind: str,
        name: str,
        parent_external_id: str | None = None,
    ) -> TrackedObject | None:
        for item in self.objects:
            if (
                item.platform == platform
                and item.kind == kind
                and item.name == name
                and item.parent_external_id == parent_external_id
            ):
                return item
        return None
