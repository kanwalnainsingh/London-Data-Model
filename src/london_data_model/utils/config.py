"""Minimal config loading utilities for the scaffolded project."""

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from london_data_model.settings import CONFIGS_DIR
from london_data_model.types import AreaConfig


def _parse_scalar(value: str) -> Any:
    raw = value.strip()
    if raw == "":
        return None
    if raw.lower() in ("null", "none"):
        return None
    if raw.lower() == "true":
        return True
    if raw.lower() == "false":
        return False
    try:
        if "." in raw:
            return float(raw)
        return int(raw)
    except ValueError:
        return raw


def load_simple_yaml(path: Path) -> Dict[str, Any]:
    """Parse a small subset of YAML used by the scaffold config files."""
    root: Dict[str, Any] = {}
    stack: List[Tuple[int, Dict[str, Any]]] = [(-1, root)]

    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue

        indent = len(line) - len(line.lstrip(" "))
        key, _, value = line.strip().partition(":")

        while stack and indent <= stack[-1][0]:
            stack.pop()

        current = stack[-1][1]
        parsed_value = _parse_scalar(value)

        if parsed_value is None and value == "" and line.rstrip().endswith(":") and not key in (
            "latitude",
            "longitude",
        ):
            child: Dict[str, Any] = {}
            current[key] = child
            stack.append((indent, child))
            continue

        current[key] = parsed_value

    return root


def resolve_area_config_path(area: str, config_path: Optional[Path] = None) -> Path:
    if config_path is not None:
        return config_path
    return CONFIGS_DIR / "areas" / "{0}.yml".format(area.lower())


def load_area_config(area: str, config_path: Optional[Path] = None) -> AreaConfig:
    path = resolve_area_config_path(area=area, config_path=config_path)
    data = load_simple_yaml(path)
    return AreaConfig(
        area_id=str(data["area_id"]),
        area_type=str(data["area_type"]),
        label=str(data["label"]),
        search_point_method=str(data["search_point_method"]),
        search_point_name=data.get("search_point_name"),
        search_point_source_type=data.get("search_point_source_type"),
        search_point_source_reference=data.get("search_point_source_reference"),
        search_point_notes=data.get("search_point_notes"),
        latitude=data.get("latitude"),
        longitude=data.get("longitude"),
        la_code=data.get("la_code"),
    )


def load_london_registry() -> Dict[str, Any]:
    """Load the London boroughs registry from configs/areas/london_boroughs.yml."""
    path = CONFIGS_DIR / "areas" / "london_boroughs.yml"
    return load_simple_yaml(path)


def list_london_borough_ids(registry: Optional[Dict[str, Any]] = None) -> List[str]:
    """Return sorted list of borough area_ids from the registry."""
    if registry is None:
        registry = load_london_registry()
    boroughs = registry.get("boroughs", {})
    return sorted(boroughs.keys())


def load_borough_configs(borough_ids: Optional[List[str]] = None) -> List[AreaConfig]:
    """Load AreaConfigs for specified boroughs (or all London boroughs if None)."""
    ids = borough_ids or list_london_borough_ids()
    return [load_area_config(area=bid) for bid in ids]


def load_pipeline_config() -> Dict[str, Any]:
    return load_simple_yaml(CONFIGS_DIR / "pipeline" / "schools.yml")


def load_threshold_config() -> Dict[str, Any]:
    return load_simple_yaml(CONFIGS_DIR / "thresholds" / "accessibility.yml")
