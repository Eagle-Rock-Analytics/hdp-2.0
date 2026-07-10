"""Shared stage exit codes for Batch/Step Functions orchestration."""

from enum import IntEnum


class StageExit(IntEnum):
    """Three-state outcome contract for stage entrypoints."""

    SUCCESS = 0
    FAILURE = 1
    NOOP = 3
