import pytest

from nexus.domain.idempotency import IdempotencyKey


def test_idempotency_key_rejects_empty_value():
    with pytest.raises(ValueError, match="must not be empty"):
        IdempotencyKey("")
