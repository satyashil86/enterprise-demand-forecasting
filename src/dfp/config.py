"""Configuration loading and environment resolution.

Pure Python (no Spark dependency) so it can be unit-tested locally and reused by
every notebook. Notebooks pass the `env` widget value; jobs pass it as a parameter.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

CONFIG_FILES = (
    "environment_config.yml",
    "source_config.yml",
    "schema_config.yml",
    "model_config.yml",
    "forecast_config.yml",
)


def find_config_dir(start: Optional[Path] = None) -> Path:
    """Locate the repo's config/ directory by walking up from `start` (or CWD).

    In Databricks Git folders / bundle deployments the notebook CWD is the notebook's
    own directory (src/), so config/ is one level up.
    """
    env_override = os.environ.get("DFP_CONFIG_DIR")
    if env_override:
        return Path(env_override)
    here = Path(start or os.getcwd()).resolve()
    for candidate in [here, *here.parents]:
        cfg = candidate / "config"
        if (cfg / "environment_config.yml").exists():
            return cfg
    raise FileNotFoundError(f"config/environment_config.yml not found above {here}")


def load_yaml(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


@dataclass
class ProjectConfig:
    env: str
    catalog: str
    catalog_fallback: Optional[str]
    secret_scope: str
    raw: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # ---- convenience accessors -------------------------------------------------
    @property
    def environment(self) -> Dict[str, Any]:
        return self.raw["environment_config"]

    @property
    def sources(self) -> Dict[str, Any]:
        return self.raw["source_config"]["sources"]

    @property
    def schemas(self) -> Dict[str, str]:
        return self.environment["schemas"]

    @property
    def volumes(self) -> Dict[str, str]:
        return self.environment["volumes"]

    @property
    def forecast(self) -> Dict[str, Any]:
        return self.raw["forecast_config"]

    @property
    def models(self) -> Dict[str, Any]:
        return self.raw["model_config"]

    @property
    def expected_schemas(self) -> Dict[str, Dict[str, str]]:
        return self.raw["schema_config"]

    def env_setting(self, key: str, default: Any = None) -> Any:
        return self.environment["environments"][self.env].get(key, default)

    def with_catalog(self, catalog: str) -> "ProjectConfig":
        """Return a copy bound to a different catalog (used when falling back)."""
        return ProjectConfig(self.env, catalog, None, self.secret_scope, self.raw)

    # ---- naming helpers ---------------------------------------------------------
    def table(self, qualified: str) -> str:
        """'bronze.sales_raw' -> '`demand_planning`.`bronze`.`sales_raw`'."""
        schema, name = qualified.split(".", 1)
        return f"`{self.catalog}`.`{schema}`.`{name}`"

    def volume_path(self, schema: str, volume: str, *parts: str) -> str:
        base = f"/Volumes/{self.catalog}/{schema}/{volume}"
        return "/".join([base, *[p.strip("/") for p in parts if p]])

    @property
    def landing_root(self) -> str:
        return self.volume_path("raw", "source_files", "landing")

    @property
    def staging_root(self) -> str:
        return self.volume_path("raw", "source_files", "staging")

    @property
    def ground_truth_root(self) -> str:
        """Synthetic answer key. Never read by pipelines/models; only by validation."""
        return self.volume_path("raw", "source_files", "_synthetic_ground_truth")

    @property
    def checkpoint_root(self) -> str:
        return self.volume_path("raw", "checkpoints")


def load_config(env: Optional[str] = None, config_dir: Optional[Path] = None) -> ProjectConfig:
    cfg_dir = Path(config_dir) if config_dir else find_config_dir()
    raw = {name.replace(".yml", ""): load_yaml(cfg_dir / name) for name in CONFIG_FILES}
    env_cfg = raw["environment_config"]
    env = env or os.environ.get("DFP_ENV") or env_cfg.get("default_env", "dev")
    if env not in env_cfg["environments"]:
        raise ValueError(f"Unknown env '{env}'. Valid: {list(env_cfg['environments'])}")
    e = env_cfg["environments"][env]
    return ProjectConfig(
        env=env,
        catalog=e["catalog"],
        catalog_fallback=e.get("catalog_fallback"),
        secret_scope=e["secret_scope"],
        raw=raw,
    )
