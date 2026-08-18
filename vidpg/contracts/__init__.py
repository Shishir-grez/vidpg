"""Version-neutral contracts shared by VidPG experiments."""

from .events import StageEvent, StageName, StageOutcome
from .frame import Codec, FrameEnvelope, FrameMeta, ValidationResult
from .manifest import DeliverySemantics, RunManifest
from .summary import ResultSummary

__all__ = [
    "Codec",
    "DeliverySemantics",
    "FrameEnvelope",
    "FrameMeta",
    "ResultSummary",
    "RunManifest",
    "StageEvent",
    "StageName",
    "StageOutcome",
    "ValidationResult",
]
