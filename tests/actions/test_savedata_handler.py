import pytest
from actions.savedata.handler import _get_unique_items, _create_set_data_func, _ensure_list
from actions.savedata.config import WriteMode


class TestGetUniqueItems:
    """Unit tests for _get_unique_items helper function."""

    def test_hashable_types(self):
        """Test deduplication with hashable types (str, int)."""
        assert _get_unique_items([1, 2, 3], [2, 4]) == [1, 3]
        assert _get_unique_items(["a", "b", "c"], ["b", "c"]) == ["a"]

    def test_empty_current(self):
        """When current is empty, all new items should be returned."""
        assert _get_unique_items([1, 2, 3], []) == [1, 2, 3]

    def test_empty_new(self):
        """When new is empty, result should be empty."""
        assert _get_unique_items([], [1, 2, 3]) == []

    def test_all_duplicates(self):
        """When all new items already exist, result should be empty."""
        assert _get_unique_items([1, 2], [1, 2, 3]) == []

    def test_unhashable_types_dicts(self):
        """Test deduplication with unhashable types (dict)."""
        new_items = [{"id": 1}, {"id": 2}, {"id": 3}]
        current_items = [{"id": 1}, {"id": 2}]
        assert _get_unique_items(new_items, current_items) == [{"id": 3}]

    def test_preserves_order(self):
        """New items order should be preserved."""
        assert _get_unique_items(["c", "a", "b"], ["b"]) == ["c", "a"]


class TestPrependNewWriteMode:
    """Integration tests for PREPEND_NEW write mode."""

    def test_prepend_new_basic(self):
        """Test basic PREPEND_NEW behavior."""
        set_data_func = _create_set_data_func(WriteMode.PREPEND_NEW)
        current = ["a", "b", "c"]
        new = ["d", "b", "e"]
        _, result = set_data_func(current, new)
        assert result == ["d", "e", "a", "b", "c"]

    def test_prepend_new_with_none_current(self):
        """PREPEND_NEW with None current should behave like PREPEND."""
        set_data_func = _create_set_data_func(WriteMode.PREPEND_NEW)
        current = None
        new = ["x", "y"]
        _, result = set_data_func(current, new)
        assert result == ["x", "y"]

    def test_prepend_new_all_duplicates(self):
        """When all new items exist, current should remain unchanged."""
        set_data_func = _create_set_data_func(WriteMode.PREPEND_NEW)
        current = [1, 2, 3]
        new = [1, 2]
        _, result = set_data_func(current, new)
        assert result == [1, 2, 3]

    def test_prepend_new_single_value(self):
        """Single non-list value should be wrapped and deduplicated."""
        set_data_func = _create_set_data_func(WriteMode.PREPEND_NEW)
        current = ["a", "b"]
        new = "c"  # Single value, not a list
        _, result = set_data_func(current, new)
        assert result == ["c", "a", "b"]

    def test_prepend_new_single_duplicate(self):
        """Single duplicate value should not be added."""
        set_data_func = _create_set_data_func(WriteMode.PREPEND_NEW)
        current = ["a", "b"]
        new = "a"  # Already exists
        _, result = set_data_func(current, new)
        assert result == ["a", "b"]