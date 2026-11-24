import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


@pytest.fixture(scope="session")
def config_dir():
    return ROOT / "config"


@pytest.fixture(scope="session")
def synthetic_small(tmp_path_factory):
    """Generate the default 'small' dataset once per test session (~20 s) and write it to disk."""
    from dfp.synthetic import SyntheticConfig, generate_all

    root = tmp_path_factory.mktemp("synthetic")
    cfg = SyntheticConfig.from_scale("small")
    gen, bb, manifest = generate_all(cfg, str(root / "staging"), str(root / "_synthetic_ground_truth"))
    return {"cfg": cfg, "gen": gen, "bb": bb, "manifest": manifest, "root": root}
