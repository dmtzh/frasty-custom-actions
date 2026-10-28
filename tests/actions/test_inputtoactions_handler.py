import pytest
from expression import Result

from actions.inputtoactions.handler import InputToActionsHandler
from actions.inputtoactions.config import InputToActionsConfig


class TestInputToActionsHandlerValidateInput:
    """Unit tests for InputToActionsHandler.validate_input with indexed errors."""

    @pytest.fixture
    def handler(self):
        return InputToActionsHandler()

    @pytest.fixture
    def config(self):
        """Minimal valid config for validate_input (not used by implementation)."""
        return InputToActionsConfig(actions=[{"action": "test"}])

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

    def test_valid_single_element(self, handler, config):
        dto_list = [{"key": "value"}]
        res = handler.validate_input(config, dto_list)

        result = self._get_ok(res)
        assert result == dto_list
        assert result is dto_list  # Original list returned, not a copy

    def test_valid_multiple_elements(self, handler, config):
        dto_list = [
            {"id": 1, "name": "alice"},
            {"id": 2, "name": "bob"},
            {"id": 3, "name": "charlie"},
        ]
        res = handler.validate_input(config, dto_list)

        result = self._get_ok(res)
        assert len(result) == 3
        assert result[1]["name"] == "bob"

    def test_valid_empty_dict_element(self, handler, config):
        """Empty dict is a valid DataDto — templates may not require fields."""
        dto_list = [{}]
        res = handler.validate_input(config, dto_list)

        result = self._get_ok(res)
        assert result == [{}]

    def test_valid_mixed_content_dicts(self, handler, config):
        """Dicts with various value types are valid."""
        dto_list = [
            {"str": "value", "int": 42, "float": 3.14},
            {"bool": True, "none": None, "list": [1, 2, 3]},
            {"nested": {"key": "value"}},
        ]
        res = handler.validate_input(config, dto_list)

        result = self._get_ok(res)
        assert len(result) == 3

    # --- Empty list error ---

    def test_empty_list_returns_error(self, handler, config):
        res = handler.validate_input(config, [])

        err = self._get_error(res)
        assert isinstance(err, str)
        assert err == "'input_data' is missing"

    def test_empty_list_error_is_formatted_string(self, handler, config):
        res = handler.validate_input(config, [])

        err = self._get_error(res)
        # Error must be a formatted string, not a raw ValueErr object
        assert isinstance(err, str)
        assert "missing" in err

    # --- Invalid element type errors ---

    def test_invalid_element_string(self, handler, config):
        res = handler.validate_input(config, ["not_a_dict"])

        err = self._get_error(res)
        assert isinstance(err, str)
        assert "input_data[0].item" in err
        assert "not_a_dict" in err
        assert "invalid" in err

    def test_invalid_element_integer(self, handler, config):
        res = handler.validate_input(config, [42])

        err = self._get_error(res)
        assert isinstance(err, str)
        assert "input_data[0].item" in err
        assert "42" in err

    def test_invalid_element_none(self, handler, config):
        res = handler.validate_input(config, [None])

        err = self._get_error(res)
        assert isinstance(err, str)
        assert "input_data[0].item" in err
        assert "None" in err

    def test_invalid_element_list(self, handler, config):
        res = handler.validate_input(config, [[1, 2, 3]])

        err = self._get_error(res)
        assert isinstance(err, str)
        assert "input_data[0].item" in err

    def test_invalid_element_bool(self, handler, config):
        res = handler.validate_input(config, [True])

        err = self._get_error(res)
        assert isinstance(err, str)
        assert "input_data[0].item" in err

    # --- Indexed error accumulation ---

    def test_multiple_invalid_elements_accumulate(self, handler, config):
        dto_list = ["bad1", {"valid": True}, "bad2", 42]
        res = handler.validate_input(config, dto_list)

        err = self._get_error(res)
        assert isinstance(err, str)

        # Errors at indices 0, 2, 3; index 1 is valid
        assert "input_data[0].item" in err
        assert "input_data[2].item" in err
        assert "input_data[3].item" in err
        # Valid element at index 1 should NOT appear in errors
        assert "input_data[1]" not in err

    def test_all_elements_invalid(self, handler, config):
        dto_list = ["a", "b", "c"]
        res = handler.validate_input(config, dto_list)

        err = self._get_error(res)
        assert isinstance(err, str)
        assert "input_data[0].item" in err
        assert "input_data[1].item" in err
        assert "input_data[2].item" in err

    def test_first_element_invalid_others_valid(self, handler, config):
        dto_list = ["invalid", {"ok": 1}, {"ok": 2}]
        res = handler.validate_input(config, dto_list)

        err = self._get_error(res)
        assert "input_data[0].item" in err
        assert "input_data[1]" not in err
        assert "input_data[2]" not in err

    def test_last_element_invalid_others_valid(self, handler, config):
        dto_list = [{"ok": 1}, {"ok": 2}, "invalid"]
        res = handler.validate_input(config, dto_list)

        err = self._get_error(res)
        assert "input_data[0]" not in err
        assert "input_data[1]" not in err
        assert "input_data[2].item" in err

    # --- Error format consistency ---

    def test_error_message_format_single(self, handler, config):
        res = handler.validate_input(config, ["bad"])

        err = self._get_error(res)
        # Expected: "'input_data[0].item' value 'bad' is invalid"
        assert err.startswith("'input_data[0].item' value")
        assert err.endswith("is invalid")

    def test_error_message_format_multiple(self, handler, config):
        res = handler.validate_input(config, ["a", "b"])

        err = self._get_error(res)
        # Multiple errors joined with comma
        assert ", " in err
        parts = err.split(", ")
        assert len(parts) == 2

    def test_error_preserves_original_value_repr(self, handler, config):
        """Invalid values should appear in error via repr()."""
        res = handler.validate_input(config, [{"nested": "but_not_dict"}])
        # This is actually valid — dict is dict
        self._get_ok(res)

        res = handler.validate_input(config, [["list_not_dict"]])
        err = self._get_error(res)
        assert "['list_not_dict']" in err

    # --- Return type contract ---

    def test_success_returns_original_list_reference(self, handler, config):
        """On success, the original dto_list is returned unchanged."""
        dto_list = [{"a": 1}]
        res = handler.validate_input(config, dto_list)

        result = self._get_ok(res)
        assert result is dto_list

    def test_error_returns_string_not_raw_error(self, handler, config):
        """validate_input must return formatted string errors, not ValueErr objects."""
        res = handler.validate_input(config, ["bad"])

        err = self._get_error(res)
        assert isinstance(err, str)
        # Must NOT be a tuple of ValueErr or a raw ValueErr
        assert not isinstance(err, tuple)

    # --- Edge cases ---

    def test_single_valid_among_many_invalid(self, handler, config):
        dto_list = ["bad1", {"only_valid": True}, "bad2", None, 123]
        res = handler.validate_input(config, dto_list)

        err = self._get_error(res)
        # 4 errors: indices 0, 2, 3, 4
        assert "input_data[0].item" in err
        assert "input_data[2].item" in err
        assert "input_data[3].item" in err
        assert "input_data[4].item" in err
        assert "input_data[1]" not in err

    def test_large_list_with_single_invalid(self, handler, config):
        dto_list = [{"idx": i} for i in range(100)]
        dto_list[50] = "invalid_at_50" # type: ignore
        res = handler.validate_input(config, dto_list)

        err = self._get_error(res)
        assert "input_data[50].item" in err
        # Only one error expected
        assert err.count("input_data[") == 1
