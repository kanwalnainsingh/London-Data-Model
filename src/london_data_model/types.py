"""Shared types for pipeline inputs and outputs."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass
class PipelineContext:
    """Run-time context for a pipeline invocation."""

    pipeline_name: str
    area: str
    config_path: Optional[Path] = None


@dataclass
class PipelineResult:
    """Minimal result object for scaffolded pipeline runs."""

    pipeline_name: str
    area: str
    status: str
    message: str
    artifacts: Dict[str, Any] = field(default_factory=dict)
