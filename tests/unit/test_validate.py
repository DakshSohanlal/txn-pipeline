import json
from datetime import UTC, datetime, timedelta

import pytest

from consumer.validate import InvalidEventError, validate_event


def make_event(**overrides):
    event = {
        "event_id": "3f0c1c2e-8a57-4c52-9d3e-0a1b2c3d4e5f",
        "user_id": "user_001",
        "type": "card",
        "amount_minor": 1250,
        "currency": "GBP",
        "merchant_name": "Pret A Manger",
        "merchant_category": "food",
        "occurred_at": "2026-01-15T12:30:00+00:00",
    }
    event.update(overrides)
    return event


def test_valid_event_is_parsed():
    ev = validate_event(make_event())
    assert ev.user_id == "user_001"
    assert ev.amount_minor == 1250
    assert ev.currency == "GBP"
    assert ev.occurred_at.tzinfo is not None


def test_accepts_json_bytes():
    ev = validate_event(json.dumps(make_event()).encode())
    assert ev.type == "card"


@pytest.mark.parametrize("field", ["event_id", "user_id", "type", "amount_minor", "currency", "occurred_at"])
def test_missing_required_field_is_rejected(field):
    raw = make_event()
    del raw[field]
    with pytest.raises(InvalidEventError):
        validate_event(raw)


@pytest.mark.parametrize("amount", [0, -1, -500])
def test_non_positive_amount_is_rejected(amount):
    with pytest.raises(InvalidEventError):
        validate_event(make_event(amount_minor=amount))


@pytest.mark.parametrize("amount", [12.5, "1250", True, None])
def test_amount_must_be_a_true_integer(amount):
    with pytest.raises(InvalidEventError):
        validate_event(make_event(amount_minor=amount))


@pytest.mark.parametrize("currency", ["gbp", "GB", "GBPP", "XXX", "", 123])
def test_invalid_currency_is_rejected(currency):
    with pytest.raises(InvalidEventError):
        validate_event(make_event(currency=currency))


def test_unknown_type_is_rejected():
    with pytest.raises(InvalidEventError):
        validate_event(make_event(type="crypto"))


def test_bad_uuid_is_rejected():
    with pytest.raises(InvalidEventError):
        validate_event(make_event(event_id="not-a-uuid"))


def test_naive_timestamp_is_rejected():
    with pytest.raises(InvalidEventError):
        validate_event(make_event(occurred_at="2026-01-15T12:30:00"))


def test_far_future_timestamp_is_rejected():
    future = (datetime.now(UTC) + timedelta(days=2)).isoformat()
    with pytest.raises(InvalidEventError):
        validate_event(make_event(occurred_at=future))


def test_empty_user_id_is_rejected():
    with pytest.raises(InvalidEventError):
        validate_event(make_event(user_id="  "))


@pytest.mark.parametrize("payload", [b"{not json", b"[1, 2, 3]", b'"hello"', b""])
def test_malformed_payload_is_rejected(payload):
    with pytest.raises(InvalidEventError):
        validate_event(payload)


def test_error_carries_a_reason_for_the_dlq():
    with pytest.raises(InvalidEventError) as exc:
        validate_event(make_event(amount_minor=-5))
    assert "amount_minor" in exc.value.reason
