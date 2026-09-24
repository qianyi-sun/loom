"""Stable errors shared by delivery exports and canonical Trial bundle readers."""

from __future__ import annotations

from typing import Any


class DeliveryExportError(Exception):
    """Base class for user-actionable delivery export failures."""

    code = "delivery_export_failed"
    status_code = 409

    def __init__(self, detail: dict[str, Any]) -> None:
        super().__init__(self.code)
        self.detail = {"code": self.code, **detail}


class MissingDeliveryObjectsError(DeliveryExportError):
    code = "delivery_export_objects_missing"


class UnreadableDeliveryObjectsError(DeliveryExportError):
    code = "delivery_export_objects_unreadable"


class CorruptDeliveryObjectsError(DeliveryExportError):
    code = "delivery_export_objects_corrupt"


class UnresolvedDeliveryTrialsError(DeliveryExportError):
    code = "delivery_export_unresolved_trials"


class InvalidDeliveryBatchFamilyError(DeliveryExportError):
    code = "delivery_export_invalid_batch_family"
    status_code = 400


class TerminalStateMismatchError(DeliveryExportError):
    code = "delivery_export_terminal_state_mismatch"
