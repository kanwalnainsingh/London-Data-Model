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
class SourceProvenance:
    """Lineage record for one raw data source used by the pipeline.

    Written to docs/data/sources.json so the dashboard can show users exactly
    which data was used, where it came from, when it was fetched, and how fresh
    the underlying source was at fetch time.
    """
    # --- Identity ---
    source_name: str                    # machine key, e.g. "gias_establishments"
    label: str                          # human label, e.g. "Get Information About Schools (GIAS)"
    source_type: str                    # same as source_name (kept for symmetry with SourceDescriptor)
    source_category: str                # "administrative_register" | "inspection_data" | "performance_statistics"

    # --- Publisher / licence ---
    publisher: str                      # e.g. "Department for Education (DfE)"
    licence: str                        # e.g. "Open Government Licence v3.0"
    home_url: str                       # canonical landing page
    update_frequency: str               # e.g. "Daily", "Monthly", "Annual"
    coverage: str                       # e.g. "All state-funded schools in England"
    description: str                    # one-sentence description for the UI
    fields_used: List[str] = field(default_factory=list)  # column names pulled from the source

    # --- Fetch provenance (populated at pipeline run time) ---
    url: Optional[str] = None           # URL the file was downloaded from
    fetched_at: Optional[str] = None    # ISO-8601 timestamp: when was this file last written to disk?
    file_path: Optional[str] = None     # relative path under project root
    file_size_bytes: Optional[int] = None
    file_size_mb: Optional[float] = None

    # --- Source-side freshness ---
    source_date: Optional[str] = None   # ISO date embedded in the source filename / URL
    academic_year: Optional[str] = None # e.g. "2022-23" for performance tables
    status: str = "not_configured"      # "loaded" | "skipped" | "missing" | "not_configured"
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


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
