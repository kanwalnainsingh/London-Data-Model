"""Shared types for pipeline inputs and outputs."""

from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


SCHOOL_OUTPUT_FIELDS = [
    "school_name",
    "school_urn",
    "address",
    "postcode",
    "latitude",
    "longitude",
    "phase",
    "establishment_type",
    "is_open",
    "distance_km",
    "accessibility_band",
    "proximity_score",
    "ofsted_rating_latest",
    "ofsted_inspection_date_latest",
    "ofsted_report_url",
    # KS4 (GCSE) — secondary and all-through schools
    "ks4_progress8",
    "ks4_attainment8",
    "ks4_strong_pass_pct",
    "ks4_standard_pass_pct",
    # KS5 (A-level) — secondary/all-through with sixth form
    "ks5_avg_point_score",
    "ks5_a_star_a_pct",
    "ks5_pass_rate_pct",
    "ks5_entries",
    "data_quality_status",
    "data_quality_flags",
]


@dataclass
class AreaConfig:
    area_id: str
    area_type: str
    label: str
    search_point_method: str
    search_point_name: Optional[str] = None
    search_point_source_type: Optional[str] = None
    search_point_source_reference: Optional[str] = None
    search_point_notes: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    la_code: Optional[int] = None


@dataclass
class SourceDescriptor:
    source_name: str
    source_type: str
    source_path: Optional[str]
    status: str
    notes: List[str] = field(default_factory=list)


@dataclass
class SchoolRecord:
    school_name: str = ""
    school_urn: str = ""
    address: str = ""
    postcode: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    phase: Optional[str] = None
    establishment_type: str = ""
    is_open: bool = True
    distance_km: Optional[float] = None
    accessibility_band: Optional[str] = None
    proximity_score: Optional[float] = None
    ofsted_rating_latest: Optional[str] = None
    ofsted_inspection_date_latest: Optional[str] = None
    ofsted_report_url: Optional[str] = None
    # KS4 (GCSE) results — populated for secondary and all-through schools
    ks4_progress8: Optional[float] = None           # Progress 8 score (-4 to +4 range)
    ks4_attainment8: Optional[float] = None         # Attainment 8 score (0–90 range)
    ks4_strong_pass_pct: Optional[float] = None     # % grade 5+ in English & Maths
    ks4_standard_pass_pct: Optional[float] = None   # % grade 4+ in English & Maths
    # KS5 (A-level) results — populated for schools with a sixth form
    ks5_avg_point_score: Optional[float] = None     # Average point score per A-level entry
    ks5_a_star_a_pct: Optional[float] = None        # % A*/A grades at A-level
    ks5_pass_rate_pct: Optional[float] = None       # % A*–E pass rate
    ks5_entries: Optional[int] = None               # Total A-level student entries
    data_quality_status: str = "partial"
    data_quality_flags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PipelineContext:
    """Run-time context for a pipeline invocation."""

    pipeline_name: str
    area: str
    run_id: str
    started_at: datetime
    config_path: Optional[Path]
    area_config: AreaConfig
    pipeline_config: Dict[str, Any]
    threshold_config: Dict[str, Any]


@dataclass
class ExtractResult:
    records: List[Dict[str, Any]] = field(default_factory=list)
    sources: List[SourceDescriptor] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


@dataclass
class TransformResult:
    records: List[SchoolRecord] = field(default_factory=list)
    excluded_record_count: int = 0
    notes: List[str] = field(default_factory=list)


@dataclass
class ValidateResult:
    records: List[SchoolRecord] = field(default_factory=list)
    quality_summary: Dict[str, int] = field(
        default_factory=lambda: {"complete": 0, "partial": 0, "poor": 0}
    )
    notes: List[str] = field(default_factory=list)


@dataclass
class PublishResult:
    output_files: Dict[str, str] = field(default_factory=dict)
    record_count: int = 0
    notes: List[str] = field(default_factory=list)


@dataclass
class PipelineResult:
    pipeline_name: str
    area: str
    status: str
    message: str
    artifacts: Dict[str, Any] = field(default_factory=dict)
