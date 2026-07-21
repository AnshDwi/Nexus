from nexus.application.use_cases.process_idempotently import process_idempotently
from nexus.domain.idempotency import IdempotencyKey


class _FakeInboxRepository:
    def __init__(self) -> None:
        self.seen: set[str] = set()

    async def mark_processed_if_new(self, key: IdempotencyKey) -> bool:
        if key.value in self.seen:
            return False
        self.seen.add(key.value)
        return True


async def test_process_idempotently_runs_handler_once_for_new_key():
    repository = _FakeInboxRepository()
    calls = 0

    async def handler() -> None:
        nonlocal calls
        calls += 1

    ran = await process_idempotently(repository, IdempotencyKey("order-1"), handler)

    assert ran is True
    assert calls == 1


async def test_process_idempotently_skips_handler_for_duplicate_key():
    repository = _FakeInboxRepository()
    calls = 0

    async def handler() -> None:
        nonlocal calls
        calls += 1

    key = IdempotencyKey("order-1")
    await process_idempotently(repository, key, handler)
    ran_again = await process_idempotently(repository, key, handler)

    assert ran_again is False
    assert calls == 1
