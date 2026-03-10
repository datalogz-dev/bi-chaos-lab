from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from bi_chaos_lab.manifest import Manifest
from bi_chaos_lab.scenario_engine import AssetPlan, SeedPlan
from bi_chaos_lab.state import StateFile


class Provider(ABC):
    name: str

    def __init__(self, manifest: Manifest, state: StateFile) -> None:
        self.manifest = manifest
        self.state = state

    @abstractmethod
    def validate(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def seed(self, plan: SeedPlan, *, dry_run: bool) -> None:
        raise NotImplementedError

    @abstractmethod
    def evolve(self, plan: list[AssetPlan], *, dry_run: bool) -> None:
        raise NotImplementedError

    @abstractmethod
    def teardown(self, *, dry_run: bool) -> None:
        raise NotImplementedError

    def template_path(self, relative_path: str) -> Path:
        return (self.manifest.root / relative_path).resolve()
