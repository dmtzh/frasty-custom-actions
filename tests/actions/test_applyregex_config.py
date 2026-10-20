
from actions.applyregex import ApplyRegexConfig
from shared.utils.parse import DefaultValue
from shared.validation import ValueMissing, ValueInvalid


class TestApplyRegexConfigFromDict:
    """Tests for ApplyRegexConfig.from_dict validation logic."""

    # --- Success cases ---

    def test_valid_config_with_all_fields(self):
        """Should parse valid config with all fields present."""
        data = {
            "field_name": "content",
            "expression": r"\d+",
            "default_value": "N/A",
            "return_empty_result": "true"
        }
        result = ApplyRegexConfig.from_dict(data)

        assert result.is_ok()
        config = result.ok
        assert config.field_name == "content"
        assert config.expression.pattern == r"\d+"
        assert config.default_value == DefaultValue("N/A")
        assert config.return_empty_result is True

    def test_valid_config_minimal(self):
        """Should parse valid config with only required fields."""
        data = {
            "field_name": "text",
            "expression": r"[a-z]+"
        }
        result = ApplyRegexConfig.from_dict(data)

        assert result.is_ok()
        config = result.ok
        assert config.field_name == "text"
        assert config.expression.pattern == r"[a-z]+"
        assert config.default_value is None
        assert config.return_empty_result is False

    def test_valid_config_with_null_default_value(self):
        """Should treat explicit null default_value as DefaultValue(None), not absent."""
        data = {
            "field_name": "text",
            "expression": r"\d+",
            "default_value": None
        }
        result = ApplyRegexConfig.from_dict(data)

        assert result.is_ok()
        config = result.ok
        # Key present with null value -> DefaultValue(None) sentinel
        assert config.default_value == DefaultValue(None)

    def test_return_empty_result_false_explicit(self):
        """Should respect explicit return_empty_result=False."""
        data = {
            "field_name": "text",
            "expression": r"\d+",
            "return_empty_result": "false"
        }
        result = ApplyRegexConfig.from_dict(data)

        assert result.is_ok()
        assert result.ok.return_empty_result is False

    # --- Error cases: missing fields ---

    def test_missing_field_name(self):
        """Should return ValueMissing error when field_name is absent."""
        data = {
            "expression": r"\d+"
        }
        result = ApplyRegexConfig.from_dict(data)

        assert result.is_error()
        errors = result.error
        assert len(errors) == 1
        assert isinstance(errors[0], ValueMissing)
        assert errors[0].name == "field_name"

    def test_missing_expression(self):
        """Should return ValueMissing error when expression is absent."""
        data = {
            "field_name": "content"
        }
        result = ApplyRegexConfig.from_dict(data)

        assert result.is_error()
        errors = result.error
        assert len(errors) == 1
        assert isinstance(errors[0], ValueMissing)
        assert errors[0].name == "expression"

    def test_missing_multiple_fields(self):
        """Should accumulate errors for all missing fields."""
        data = {}
        result = ApplyRegexConfig.from_dict(data)

        assert result.is_error()
        errors = result.error
        assert len(errors) == 2
        error_names = {e.name for e in errors}
        assert "field_name" in error_names
        assert "expression" in error_names

    # --- Error cases: invalid values ---

    def test_empty_field_name(self):
        """Should return ValueInvalid error for empty field_name."""
        data = {
            "field_name": "",
            "expression": r"\d+"
        }
        result = ApplyRegexConfig.from_dict(data)

        assert result.is_error()
        errors = result.error
        assert len(errors) == 1
        assert isinstance(errors[0], ValueInvalid)
        assert errors[0].name == "field_name"

    def test_whitespace_only_field_name(self):
        """Should return ValueInvalid error for whitespace-only field_name."""
        data = {
            "field_name": "   ",
            "expression": r"\d+"
        }
        result = ApplyRegexConfig.from_dict(data)

        assert result.is_error()
        errors = result.error
        assert len(errors) == 1
        assert isinstance(errors[0], ValueInvalid)
        assert errors[0].name == "field_name"

    def test_invalid_expression_regex(self):
        """Should return ValueInvalid error for malformed regex expression."""
        data = {
            "field_name": "content",
            "expression": r"[invalid("
        }
        result = ApplyRegexConfig.from_dict(data)

        assert result.is_error()
        errors = result.error
        assert len(errors) == 1
        assert isinstance(errors[0], ValueInvalid)
        assert errors[0].name == "expression"

    def test_non_string_field_name(self):
        """Should return ValueInvalid error for non-string field_name."""
        data = {
            "field_name": 123,
            "expression": r"\d+"
        }
        result = ApplyRegexConfig.from_dict(data)

        assert result.is_error()
        errors = result.error
        assert len(errors) == 1
        assert isinstance(errors[0], ValueInvalid)
        assert errors[0].name == "field_name"

    def test_invalid_return_empty_result_type(self):
        """Should return ValueInvalid error for non-boolean return_empty_result."""
        data = {
            "field_name": "content",
            "expression": r"\d+",
            "return_empty_result": "not_a_bool"
        }
        result = ApplyRegexConfig.from_dict(data)

        assert result.is_error()
        errors = result.error
        assert len(errors) == 1
        assert isinstance(errors[0], ValueInvalid)
        assert errors[0].name == "return_empty_result"
