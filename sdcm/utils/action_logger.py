import logging
from typing import Dict, Optional, Any
import json


class ActionLogger:
    def __init__(self, logger: logging.Logger, source: str):
        self.logger = logger
        self.root_logger = logging.getLogger(__name__)
        self.source = source

    def _log(self, level: int, action: str, target: Optional[str] = None, trace_id: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        extra = {
            "source": self.source,
            "action": action,
        }
        message = f" source: {self.source}, action: {action}"
        if target:
            extra["target"] = target
            message += f", target: {target}"
        if trace_id:
            extra["trace_id"] = trace_id
            message += f", trace_id: {trace_id}"
        if metadata:
            extra["metadata"] = metadata
            message += f", metadata: {json.dumps(metadata)}"

        self.logger.log(level, "", extra=extra)
        self.root_logger.log(level, message)

    def info(self, action: str, target: Optional[str] = None, trace_id: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        self._log(logging.INFO, action, target, trace_id, metadata)

    def warning(self, action: str, target: Optional[str] = None, trace_id: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        self._log(logging.WARNING, action, target, trace_id, metadata)

    def error(self, action: str, target: Optional[str] = None, trace_id: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        self._log(logging.ERROR, action, target, trace_id, metadata)


logger = logging.getLogger("action_logger")
logger.setLevel(logging.INFO)


def get_action_logger(source: str) -> ActionLogger:
    return ActionLogger(logger, source)
