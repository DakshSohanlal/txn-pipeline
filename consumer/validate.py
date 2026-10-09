import json

from pydantic import ValidationError

from consumer.models import TransactionEvent


class InvalidEventError(Exception):
    """Raised when a raw message cannot be turned into a valid TransactionEvent."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def validate_event(raw: bytes | str | dict) -> TransactionEvent:
    """Pure function: parse and validate one raw message. No I/O."""
    if isinstance(raw, (bytes, str)):
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise InvalidEventError(f"malformed json: {exc}") from exc
    else:
        data = raw

    if not isinstance(data, dict):
        raise InvalidEventError("payload must be a json object")

    try:
        return TransactionEvent.model_validate(data)
    except ValidationError as exc:
        reasons = "; ".join(
            f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors()
        )
        raise InvalidEventError(reasons) from exc
