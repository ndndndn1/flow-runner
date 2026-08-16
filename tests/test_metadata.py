from __future__ import annotations

import pytest

from flow_runner.metadata import canonical_bytes, describe


def test_metadata_is_deterministic_and_contains_no_payload():
    left = describe({"secret": "do-not-store", "rows": [2, 1]}, "schema", max_bytes=1000)
    right = describe({"rows": [2, 1], "secret": "do-not-store"}, "schema", max_bytes=1000)
    assert left == right
    assert left["recordCount"] == 2
    assert "do-not-store" not in str(left)
    assert canonical_bytes({"b": 1, "a": 2}) == b'{"a":2,"b":1}'


def test_payload_limit_is_enforced():
    with pytest.raises(ValueError, match="limit"):
        describe({"value": "x" * 100}, "schema", max_bytes=10)

