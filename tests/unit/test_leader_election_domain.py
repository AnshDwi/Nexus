import pytest

from nexus.domain.leader_election import classify_transition


@pytest.mark.parametrize(
    ("was_leader", "is_leader", "expected"),
    [
        (False, True, "acquired"),
        (True, False, "lost"),
        (True, True, None),
        (False, False, None),
    ],
)
def test_classify_transition(was_leader, is_leader, expected):
    assert classify_transition(was_leader=was_leader, is_leader=is_leader) == expected
