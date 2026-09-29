"""
sh_http package.

This package is exposing small HTTP helpers used across the project,
including `extract_parameters` and `wget`.
"""

from .request_common import extract_parameters
from .wget import wget

__all__ = [
    "extract_parameters",
    "wget",
]
