import pytest

from nexus.domain.events import DomainEvent


def _make_event(**overrides):
    defaults = {
        "aggregate_id": "order-1",
        "aggregate_type": "Order",
        "event_type": "OrderCreated",
        "payload": {"total": 42},
    }
    defaults.update(overrides)
    return DomainEvent(**defaults)


def test_domain_event_defaults_are_populated():
    event = _make_event()

    assert event.event_id is not None
    assert event.correlation_id is not None
    assert event.causation_id is None
    assert event.event_version == 1
    assert event.metadata == {}


@pytest.mark.parametrize("field", ["aggregate_id", "aggregate_type", "event_type"])
def test_domain_event_rejects_empty_required_fields(field):
    with pytest.raises(ValueError, match="must not be empty"):
        _make_event(**{field: ""})
