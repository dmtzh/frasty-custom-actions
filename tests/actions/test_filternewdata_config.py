import pytest

from actions.filternewdata.handler import FilterNewDataConfig, CompareTo
from actions.filternewdata.projection import ProjectionResolver
from shared.validation import ValueMissing, ValueInvalid


class TestFilterNewDataConfigFromDictValid:
    """Group A: Valid configurations."""

    def test_full_config(self):
        """A1: Full config with all fields provided."""
        data = {"set_name": "my_set", "compare_to": "ALL", "projection": "user.id"}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_ok()
        config = result.ok
        assert config.set_name == "my_set"
        assert config.compare_to == CompareTo.ALL
        assert isinstance(config.resolver, ProjectionResolver)

    def test_without_compare_to_defaults_to_all(self):
        """A2: compare_to is optional, defaults to ALL."""
        data = {"set_name": "my_set", "projection": "user.id"}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_ok()
        config = result.ok
        assert config.compare_to == CompareTo.ALL

    def test_without_projection_defaults_to_identity(self):
        """A3: projection is optional, defaults to identity resolver."""
        data = {"set_name": "my_set", "compare_to": "LAST"}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_ok()
        config = result.ok
        assert config.compare_to == CompareTo.LAST
        assert isinstance(config.resolver, ProjectionResolver)

    def test_only_set_name(self):
        """A4: Only required field provided, all defaults applied."""
        data = {"set_name": "my_set"}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_ok()
        config = result.ok
        assert config.set_name == "my_set"
        assert config.compare_to == CompareTo.ALL
        assert isinstance(config.resolver, ProjectionResolver)

    @pytest.mark.parametrize("compare_to_value,expected", [
        ("ALL", CompareTo.ALL),
        ("LAST", CompareTo.LAST),
        ("ALL_READONLY", CompareTo.ALL_READONLY),
        ("LAST_READONLY", CompareTo.LAST_READONLY),
    ])
    def test_compare_to_valid_values(self, compare_to_value: str, expected: CompareTo):
        """A5-A8: All valid compare_to values are accepted."""
        data = {"set_name": "my_set", "compare_to": compare_to_value}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_ok()
        assert result.ok.compare_to == expected

    def test_compare_to_case_insensitive(self):
        """A5: compare_to parsing is case-insensitive."""
        data = {"set_name": "my_set", "compare_to": "all"}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_ok()
        assert result.ok.compare_to == CompareTo.ALL

    def test_compare_to_strips_whitespace(self):
        """A6: compare_to parsing strips whitespace."""
        data = {"set_name": "my_set", "compare_to": " ALL "}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_ok()
        assert result.ok.compare_to == CompareTo.ALL

    def test_set_name_strips_whitespace(self):
        """set_name is stripped by NonEmptyStr.parse."""
        data = {"set_name": "  my_set  "}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_ok()
        assert result.ok.set_name == "my_set"


class TestFilterNewDataConfigFromDictMissingFields:
    """Group B: Missing required fields."""

    def test_missing_set_name(self):
        """B1: set_name is required."""
        data = {"compare_to": "ALL"}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_error()
        errors = result.error
        assert len(errors) == 1
        assert isinstance(errors[0], ValueMissing)
        assert errors[0].name == "set_name"

    def test_empty_config(self):
        """B2: Empty config returns ValueMissing for set_name."""
        data = {}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_error()
        errors = result.error
        assert len(errors) == 1
        assert isinstance(errors[0], ValueMissing)
        assert errors[0].name == "set_name"


class TestFilterNewDataConfigFromDictInvalidValues:
    """Group C: Invalid values."""

    def test_empty_set_name(self):
        """C1: Empty string is invalid for set_name."""
        data = {"set_name": ""}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_error()
        errors = result.error
        assert len(errors) == 1
        assert isinstance(errors[0], ValueInvalid)
        assert errors[0].name == "set_name"
        assert errors[0].value == ""

    def test_whitespace_only_set_name(self):
        """C2: Whitespace-only string is invalid for set_name."""
        data = {"set_name": "   "}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_error()
        errors = result.error
        assert len(errors) == 1
        assert isinstance(errors[0], ValueInvalid)
        assert errors[0].name == "set_name"

    def test_invalid_compare_to(self):
        """C3: Unrecognized compare_to value is invalid."""
        data = {"set_name": "my_set", "compare_to": "UNKNOWN"}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_error()
        errors = result.error
        assert len(errors) == 1
        assert isinstance(errors[0], ValueInvalid)
        assert errors[0].name == "compare_to"
        assert errors[0].value == "UNKNOWN"

    def test_empty_compare_to(self):
        """C4: Empty string is invalid for compare_to."""
        data = {"set_name": "my_set", "compare_to": ""}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_error()
        errors = result.error
        assert len(errors) == 1
        assert isinstance(errors[0], ValueInvalid)
        assert errors[0].name == "compare_to"

    def test_invalid_projection_syntax(self):
        """C5: Invalid JMESPath syntax is detected at config validation."""
        data = {"set_name": "my_set", "projection": "invalid[[syntax"}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_error()
        errors = result.error
        assert len(errors) == 1
        assert isinstance(errors[0], ValueInvalid)
        assert errors[0].name == "projection"

    def test_empty_projection(self):
        """C6: Empty string is invalid for projection."""
        data = {"set_name": "my_set", "projection": ""}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_error()
        errors = result.error
        assert len(errors) == 1
        assert isinstance(errors[0], ValueInvalid)
        assert errors[0].name == "projection"


class TestFilterNewDataConfigFromDictInvalidTypes:
    """Group D: Invalid types."""

    def test_set_name_not_string(self):
        """D1: Non-string set_name is invalid."""
        data = {"set_name": 123}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_error()
        errors = result.error
        assert len(errors) == 1
        assert isinstance(errors[0], ValueInvalid)
        assert errors[0].name == "set_name"
        assert errors[0].value == 123

    def test_compare_to_not_string(self):
        """D2: Non-string compare_to is invalid."""
        data = {"set_name": "my_set", "compare_to": 42}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_error()
        errors = result.error
        assert len(errors) == 1
        assert isinstance(errors[0], ValueInvalid)
        assert errors[0].name == "compare_to"
        assert errors[0].value == 42

    def test_projection_not_string(self):
        """D3: Non-string projection is invalid."""
        data = {"set_name": "my_set", "projection": ["user.id"]}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_error()
        errors = result.error
        assert len(errors) == 1
        assert isinstance(errors[0], ValueInvalid)
        assert errors[0].name == "projection"


class TestFilterNewDataConfigFromDictMultipleErrors:
    """Group E: Multiple errors (accumulation)."""

    def test_two_errors_accumulated(self):
        """E1: Two errors are accumulated in tuple."""
        data = {"set_name": "", "compare_to": "UNKNOWN"}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_error()
        errors = result.error
        assert len(errors) == 2
        assert isinstance(errors[0], ValueInvalid)
        assert errors[0].name == "set_name"
        assert isinstance(errors[1], ValueInvalid)
        assert errors[1].name == "compare_to"

    def test_three_errors_accumulated(self):
        """E2: Three errors are accumulated in tuple."""
        data = {"set_name": "", "compare_to": "UNKNOWN", "projection": "invalid[[syntax"}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_error()
        errors = result.error
        assert len(errors) == 3
        assert isinstance(errors[0], ValueInvalid)
        assert errors[0].name == "set_name"
        assert isinstance(errors[1], ValueInvalid)
        assert errors[1].name == "compare_to"
        assert isinstance(errors[2], ValueInvalid)
        assert errors[2].name == "projection"

    def test_missing_set_name_and_invalid_compare_to(self):
        """E3: ValueMissing and ValueInvalid are accumulated together."""
        data = {"compare_to": "UNKNOWN"}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_error()
        errors = result.error
        assert len(errors) == 2
        assert isinstance(errors[0], ValueMissing)
        assert errors[0].name == "set_name"
        assert isinstance(errors[1], ValueInvalid)
        assert errors[1].name == "compare_to"

    def test_error_order_matches_field_order(self):
        """Errors are accumulated in field order: set_name, compare_to, projection."""
        data = {"set_name": "", "compare_to": "UNKNOWN", "projection": "invalid[[syntax"}
        result = FilterNewDataConfig.from_dict(data)
        
        assert result.is_error()
        errors = result.error
        assert errors[0].name == "set_name"
        assert errors[1].name == "compare_to"
        assert errors[2].name == "projection"