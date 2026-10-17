import pytest
from expression import Result

from shared.validation import ValueMissing, ValueInvalid
from actions.inputtoactions.config import InputToActionsConfig


class TestInputToActionsConfigFromDict:
    """Unit tests for InputToActionsConfig.from_dict with structured errors."""

    # --- Helpers ---

    @staticmethod
    def _get_ok(res: Result):
        """Extract Ok value from Result using pattern matching."""
        match res:
            case Result(tag="ok", ok=value):
                return value
            case _:
                pytest.fail(f"Expected Ok, got Error: {res}")

    @staticmethod
    def _get_error(res: Result):
        """Extract Error value from Result using pattern matching."""
        match res:
            case Result(tag="error", error=err):
                return err
            case _:
                pytest.fail(f"Expected Error, got Ok: {res}")

    # --- Success cases ---

    def test_valid_single_action(self):
        raw = {"actions": [{"action": "getfromjson", "query": "$.data"}]}
        res = InputToActionsConfig.from_dict(raw)

        config = self._get_ok(res)
        assert len(config.actions) == 1
        assert config.actions[0] == {"action": "getfromjson", "query": "$.data"}

    def test_valid_multiple_actions(self):
        raw = {
            "actions": [
                {"action": "getfromjson", "query": "$.a"},
                {"action": "savedata", "storage": "db"},
                {"action": "normalize", "field": "name"},
            ]
        }
        res = InputToActionsConfig.from_dict(raw)

        config = self._get_ok(res)
        assert len(config.actions) == 3
        assert config.actions[1]["action"] == "savedata"

    def test_valid_empty_list(self):
        """Empty actions list passes parsing — handler validates emptiness."""
        raw = {"actions": []}
        res = InputToActionsConfig.from_dict(raw)

        config = self._get_ok(res)
        assert config.actions == []

    # --- Missing field errors ---

    def test_missing_actions_key(self):
        raw = {"other_field": "value"}
        res = InputToActionsConfig.from_dict(raw)

        err = self._get_error(res)
        assert isinstance(err, tuple)
        assert len(err) == 1
        assert isinstance(err[0], ValueMissing)
        assert err[0].name == "actions"

    def test_missing_actions_in_empty_dict(self):
        res = InputToActionsConfig.from_dict({})

        err = self._get_error(res)
        assert isinstance(err, tuple)
        assert any(isinstance(e, ValueMissing) and e.name == "actions" for e in err)

    # --- Invalid type errors ---

    def test_actions_not_a_list(self):
        raw = {"actions": "not_a_list"}
        res = InputToActionsConfig.from_dict(raw)

        err = self._get_error(res)
        assert isinstance(err, tuple)
        assert len(err) == 1
        assert isinstance(err[0], ValueInvalid)
        assert err[0].name == "actions"
        assert err[0].value == "not_a_list"

    def test_actions_is_none(self):
        raw = {"actions": None}
        res = InputToActionsConfig.from_dict(raw)

        err = self._get_error(res)
        assert isinstance(err, tuple)
        assert any(isinstance(e, ValueInvalid) and e.name == "actions" for e in err)

    def test_actions_is_integer(self):
        raw = {"actions": 42}
        res = InputToActionsConfig.from_dict(raw)

        err = self._get_error(res)
        assert isinstance(err, tuple)
        assert any(isinstance(e, ValueInvalid) and e.name == "actions" for e in err)

    # --- Invalid item errors with indexing ---

    def test_single_invalid_item_string(self):
        raw = {"actions": ["not_a_dict"]}
        res = InputToActionsConfig.from_dict(raw)

        err = self._get_error(res)
        assert isinstance(err, tuple)
        assert len(err) == 1
        assert isinstance(err[0], ValueInvalid)
        assert err[0].name == "actions[0]"
        assert err[0].value == "not_a_dict"

    def test_single_invalid_item_none(self):
        raw = {"actions": [None]}
        res = InputToActionsConfig.from_dict(raw)

        err = self._get_error(res)
        assert isinstance(err, tuple)
        assert any(
            isinstance(e, ValueInvalid) and e.name == "actions[0]" for e in err
        )

    def test_single_invalid_item_empty_dict(self):
        raw = {"actions": [{}]}
        res = InputToActionsConfig.from_dict(raw)

        err = self._get_error(res)
        assert isinstance(err, tuple)
        assert any(
            isinstance(e, ValueInvalid) and e.name == "actions[0]" for e in err
        )

    def test_multiple_invalid_items_accumulate_errors(self):
        raw = {"actions": ["bad1", {"valid": True}, "bad2", 42]}
        res = InputToActionsConfig.from_dict(raw)

        err = self._get_error(res)
        assert isinstance(err, tuple)
        # 3 errors: index 0, 2, 3; index 1 is valid
        assert len(err) == 3

        error_names = [e.name for e in err]
        assert "actions[0]" in error_names
        assert "actions[2]" in error_names
        assert "actions[3]" in error_names
        assert not any("actions[1]" in name for name in error_names)

    def test_all_items_invalid(self):
        raw = {"actions": ["a", "b", "c"]}
        res = InputToActionsConfig.from_dict(raw)

        err = self._get_error(res)
        assert isinstance(err, tuple)
        assert len(err) == 3
        assert all(isinstance(e, ValueInvalid) for e in err)
        assert [e.name for e in err] == [
            "actions[0]",
            "actions[1]",
            "actions[2]",
        ]

    # --- Error type consistency ---

    def test_error_is_always_tuple(self):
        """Ensure error is always tuple[ValueErr, ...], never a single ValueErr."""
        for raw in [{}, {"actions": 123}, {"actions": ["bad"]}]:
            res = InputToActionsConfig.from_dict(raw)
            err = self._get_error(res)
            assert isinstance(err, tuple), f"Expected tuple for {raw}, got {type(err)}"

    def test_errors_are_frozen_dataclasses(self):
        raw = {"actions": ["bad"]}
        res = InputToActionsConfig.from_dict(raw)
        err = self._get_error(res)

        # Frozen dataclasses should be hashable
        assert hash(err[0]) is not None

    # --- Compatibility with format_value_errors ---

    def test_errors_compatible_with_format_value_errors(self):
        from shared.validation import format_value_errors

        raw = {"actions": ["bad_item", {"valid": True}, None]}
        res = InputToActionsConfig.from_dict(raw)
        err = self._get_error(res)

        formatted = format_value_errors(err)
        assert isinstance(formatted, str)
        assert "actions[0]" in formatted
        assert "actions[2]" in formatted
        assert "actions[1]" not in formatted