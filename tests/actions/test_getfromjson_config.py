import pytest
from expression import Result

# Module under test
from actions.getfromjson.config import (
    Mode,
    Parser,
    Operation,
    GetFromJsonQuery,
    GetFromJsonFilter,
    GetFromJsonOperationConfig,
    GetFromJsonConfig,
)

# Shared dependencies for assertions
from shared.validation import ValueMissing, ValueInvalid
from shared.utils.parse import DefaultValue


class TestModeEnum:
    """Tests for the Mode StrEnum and its safe parser."""

    @pytest.mark.parametrize("value,expected", [
        ("single", Mode.SINGLE),
        ("ALL", Mode.ALL),
        (" SiNgLe ", Mode.SINGLE),  # Case-insensitive and strips whitespace
        ("invalid", None),
        (None, None),
    ])
    def test_parse_mode(self, value, expected):
        # Assert that parse correctly handles case-insensitivity, stripping, and invalid values
        assert Mode.parse(value) == expected


class TestParserEnum:
    """Tests for the Parser StrEnum and its safe parser."""

    @pytest.mark.parametrize("value,expected", [
        ("jmespath", Parser.JMESPATH),
        ("jsonpath-ng", Parser.JSONPATH_NG),
        (" JMESPATH ", Parser.JMESPATH),
        ("invalid", None),
        (None, None),
    ])
    def test_parse_parser(self, value, expected):
        # Assert correct enum resolution and None fallback
        assert Parser.parse(value) == expected


class TestGetFromJsonQuery:
    """Tests for GetFromJsonQuery dataclass validation."""

    def test_from_dict_minimal_success(self):
        # Minimal valid configuration should succeed and use defaults
        data = {"query": "$.items"}
        res = GetFromJsonQuery.from_dict(data)

        assert res.is_ok()
        query = res.ok
        assert query.query == "$.items"
        assert query.output_name is None
        assert query.default_value is None
        assert query.mode == Mode.SINGLE

    def test_from_dict_full_success(self):
        # Full configuration with all optional fields populated
        data = {
            "query": "$.users",
            "output_name": "user_list",
            "default_value": [],
            "mode": "all"
        }
        res = GetFromJsonQuery.from_dict(data)

        assert res.is_ok()
        query = res.ok
        assert query.query == "$.users"
        assert query.output_name == "user_list"
        assert query.default_value == DefaultValue([])
        assert query.mode == Mode.ALL

    def test_from_dict_missing_query(self):
        # Missing required 'query' field should yield ValueMissing
        data = {"mode": "single"}
        res = GetFromJsonQuery.from_dict(data)

        assert res.is_error()
        errors = res.error
        assert any(isinstance(e, ValueMissing) and e.name == "query" for e in errors)

    def test_from_dict_invalid_mode(self):
        # Invalid mode value should yield ValueInvalid
        data = {"query": "$.test", "mode": "unknown_mode"}
        res = GetFromJsonQuery.from_dict(data)

        assert res.is_error()
        errors = res.error
        assert any(isinstance(e, ValueInvalid) and e.name == "mode" for e in errors)

    def test_from_dict_accumulates_errors(self):
        # Multiple validation failures should be accumulated, not fail-fast
        data = {"output_name": 123}  # missing query, invalid output_name type
        res = GetFromJsonQuery.from_dict(data)

        assert res.is_error()
        errors = res.error
        error_names = {e.name for e in errors if hasattr(e, 'name')}
        assert "query" in error_names
        assert "output_name" in error_names


class TestGetFromJsonFilter:
    """Tests for GetFromJsonFilter (NonEmptyStr subclass) validation."""

    def test_from_dict_success(self):
        # Valid filter string should be wrapped in GetFromJsonFilter
        data = {"filter": "status == 'active'"}
        res = GetFromJsonFilter.from_dict(data)

        assert res.is_ok()
        assert res.ok == "status == 'active'"
        assert isinstance(res.ok, GetFromJsonFilter)

    def test_from_dict_missing_filter(self):
        # Missing 'filter' key should result in ValueMissing
        data = {}
        res = GetFromJsonFilter.from_dict(data)

        assert res.is_error()
        assert any(isinstance(e, ValueMissing) and e.name == "filter" for e in res.error)

    def test_from_dict_empty_filter(self):
        # Empty or whitespace-only string should result in ValueInvalid
        data = {"filter": "   "}
        res = GetFromJsonFilter.from_dict(data)

        assert res.is_error()
        assert any(isinstance(e, ValueInvalid) and e.name == "filter" for e in res.error)


class TestGetFromJsonOperationConfig:
    """Tests for the discriminated union of Query and Filter operations."""

    def test_query_operation_default_parser(self):
        # Query operation should default to JMESPATH if parser is omitted
        data = {"query": "$.data"}
        res = GetFromJsonOperationConfig.from_dict(data)

        assert res.is_ok()
        config = res.ok
        assert config.operation == Operation.QUERY
        assert config.parser == Parser.JMESPATH
        assert isinstance(config.data, GetFromJsonQuery)

    def test_filter_operation_with_explicit_parser(self):
        # Filter operation should respect explicitly provided parser
        data = {"filter": "age > 18", "parser": "jsonpath-ng"}
        res = GetFromJsonOperationConfig.from_dict(data)

        assert res.is_ok()
        config = res.ok
        assert config.operation == Operation.FILTER
        assert config.parser == Parser.JSONPATH_NG
        assert isinstance(config.data, GetFromJsonFilter)

    def test_neither_query_nor_filter(self):
        # Providing neither 'query' nor 'filter' should yield ValueInvalid on '*'
        data = {"parser": "jmespath"}
        res = GetFromJsonOperationConfig.from_dict(data)

        assert res.is_error()
        errors = res.error
        assert any(isinstance(e, ValueInvalid) and e.name == "*" for e in errors)

    def test_both_query_and_filter(self):
        # Providing both 'query' and 'filter' simultaneously should yield ValueInvalid
        data = {"query": "$.a", "filter": "b > 0"}
        res = GetFromJsonOperationConfig.from_dict(data)

        assert res.is_error()
        errors = res.error
        assert any(isinstance(e, ValueInvalid) and e.name == "*" for e in errors)


class TestGetFromJsonConfig:
    """Tests for the root configuration object holding the pipeline sequence."""

    def test_valid_config_single_operation(self):
        # Valid config with a single query operation
        data = {
            "operations": [{"query": "$.items"}],
            "return_empty_result": "true"
        }
        res = GetFromJsonConfig.from_dict(data)

        assert res.is_ok()
        config = res.ok
        assert len(config.operations) == 1
        assert config.return_empty_result is True

    def test_valid_config_multiple_operations(self):
        # Valid config chaining multiple operations
        data = {
            "operations": [
                {"query": "$.users"},
                {"filter": "age >= 18", "parser": "jmespath"}
            ]
        }
        res = GetFromJsonConfig.from_dict(data)

        assert res.is_ok()
        config = res.ok
        assert len(config.operations) == 2
        assert config.operations[0].operation == Operation.QUERY
        assert config.operations[1].operation == Operation.FILTER
        assert config.return_empty_result is False  # Default value

    def test_missing_operations(self):
        # Missing 'operations' key should yield ValueMissing
        data = {"return_empty_result": False}
        res = GetFromJsonConfig.from_dict(data)

        assert res.is_error()
        errors = res.error
        assert any(isinstance(e, ValueMissing) and e.name == "operations" for e in errors)

    def test_empty_operations_list(self):
        # Empty list of operations should be rejected as invalid
        data = {"operations": []}
        res = GetFromJsonConfig.from_dict(data)

        assert res.is_error()
        errors = res.error
        assert any(isinstance(e, ValueInvalid) and e.name == "operations" for e in errors)

    def test_accumulating_errors_with_prefixes(self):
        # Errors inside nested operations should be prefixed with 'operations[index]'
        data = {
            "operations": [
                {"query": ""},  # Invalid query (empty string) at index 0
                {"query": "$.a", "mode": "bad_mode"}  # Invalid mode at index 1
            ]
        }
        res = GetFromJsonConfig.from_dict(data)

        assert res.is_error()
        errors = res.error

        # Extract error names to verify prefix application
        error_names = {e.name for e in errors if hasattr(e, 'name')}

        assert "operations[0].query" in error_names
        assert "operations[1].mode" in error_names