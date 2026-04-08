"""Centralised configuration for ClickEngine pipelines."""

from dataclasses import dataclass, field


@dataclass
class PipelineConfig:
    """Configuration for a ClickEngine pipeline."""
    source_location: str
    target_location: str
    read_format: str
    read_options: dict = field(default_factory=dict)
