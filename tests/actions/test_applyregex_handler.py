import re
import pytest

from actions.applyregex import ApplyRegexConfig, ApplyRegexHandler
from shared.completedresult import CompletedWith
from shared.utils.parse import DefaultValue, NonEmptyStr

class TestApplyRegexHandlerValidateConfig:
    """Tests for ApplyRegexHandler.validate_config method."""

    def test_validate_config_valid(self):
        """Should return Ok config for valid raw config."""
        handler = ApplyRegexHandler()
        raw_config = {
            "field_name": "content",
            "expression": r"\d+"
        }
        result = handler.validate_config(raw_config)

        assert result.is_ok()
        assert isinstance(result.ok, ApplyRegexConfig)

    def test_validate_config_invalid_returns_formatted_errors(self):
        """Should return formatted error string for invalid config."""
        handler = ApplyRegexHandler()
        raw_config = {
            "field_name": "",  # Invalid
            "expression": r"[invalid("  # Invalid
        }
        result = handler.validate_config(raw_config)

        assert result.is_error()
        # Error should be formatted string, not raw ValueErr
        assert isinstance(result.error, str)
        assert len(result.error) > 0


class TestApplyRegexHandlerHandle:
    """Tests for ApplyRegexHandler.handle business logic."""

    @pytest.fixture
    def handler(self):
        return ApplyRegexHandler()

    # --- Basic regex matching ---

    async def test_single_match(self, handler):
        """Should replace field with matched value."""
        config = ApplyRegexConfig(
            field_name=NonEmptyStr("text"),
            expression=re.compile(r"\d+"),
            default_value=None,
            return_empty_result=False
        )
        input_list = [{"text": "abc123def", "id": 1}]

        result = await handler.handle(config, input_list)

        assert type(result) is CompletedWith.Data
        assert result.data == [{"text": "123", "id": 1}]

    async def test_multiple_matches(self, handler):
        """Should create one DataDto per match."""
        config = ApplyRegexConfig(
            field_name=NonEmptyStr("text"),
            expression=re.compile(r"\d+"),
            default_value=None,
            return_empty_result=False
        )
        input_list = [{"text": "a1b2c3", "id": 1}]

        result = await handler.handle(config, input_list)

        assert type(result) is CompletedWith.Data
        assert result.data == [
            {"text": "1", "id": 1},
            {"text": "2", "id": 1},
            {"text": "3", "id": 1}
        ]

    async def test_no_matches_without_default(self, handler):
        """Should drop element when no matches and no default_value."""
        config = ApplyRegexConfig(
            field_name=NonEmptyStr("text"),
            expression=re.compile(r"\d+"),
            default_value=None,
            return_empty_result=False
        )
        input_list = [{"text": "no digits", "id": 1}]

        result = await handler.handle(config, input_list)

        assert type(result) is CompletedWith.NoData

    async def test_no_matches_with_default(self, handler):
        """Should substitute default_value when no matches."""
        config = ApplyRegexConfig(
            field_name=NonEmptyStr("text"),
            expression=re.compile(r"\d+"),
            default_value=DefaultValue("N/A"),
            return_empty_result=False
        )
        input_list = [{"text": "no digits", "id": 1}]

        result = await handler.handle(config, input_list)

        assert type(result) is CompletedWith.Data
        assert result.data == [{"text": "N/A", "id": 1}]

    async def test_matches_ignore_default_value(self, handler):
        """Should ignore default_value when matches are found."""
        config = ApplyRegexConfig(
            field_name=NonEmptyStr("text"),
            expression=re.compile(r"\d+"),
            default_value=DefaultValue("N/A"),
            return_empty_result=False
        )
        input_list = [{"text": "abc123", "id": 1}]

        result = await handler.handle(config, input_list)

        assert type(result) is CompletedWith.Data
        assert result.data == [{"text": "123", "id": 1}]

    # --- Field missing cases ---

    async def test_field_missing_in_input(self, handler):
        """Should preserve element unchanged when field_name is missing."""
        config = ApplyRegexConfig(
            field_name=NonEmptyStr("missing_field"),
            expression=re.compile(r"\d+"),
            default_value=None,
            return_empty_result=False
        )
        input_list = [{"text": "abc", "id": 1}]

        result = await handler.handle(config, input_list)

        assert type(result) is CompletedWith.Data
        assert result.data == [{"text": "abc", "id": 1}]

    # --- Empty input handling ---

    async def test_empty_input_with_return_empty_result_true(self, handler):
        """Should return Data with empty list when return_empty_result=True."""
        config = ApplyRegexConfig(
            field_name=NonEmptyStr("text"),
            expression=re.compile(r"\d+"),
            default_value=None,
            return_empty_result=True
        )

        result = await handler.handle(config, [])

        assert type(result) is CompletedWith.Data
        assert result.data == []

    async def test_empty_input_with_return_empty_result_false(self, handler):
        """Should return NoData when return_empty_result=False."""
        config = ApplyRegexConfig(
            field_name=NonEmptyStr("text"),
            expression=re.compile(r"\d+"),
            default_value=None,
            return_empty_result=False
        )

        result = await handler.handle(config, [])

        assert type(result) is CompletedWith.NoData

    # --- Complex patterns ---

    async def test_group_matching(self, handler):
        """Should match entire pattern with group(0)."""
        config = ApplyRegexConfig(
            field_name=NonEmptyStr("email"),
            expression=re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"),
            default_value=None,
            return_empty_result=False
        )
        input_list = [{"email": "contact: user@example.com", "id": 1}]

        result = await handler.handle(config, input_list)

        assert type(result) is CompletedWith.Data
        assert result.data == [{"email": "user@example.com", "id": 1}]

    async def test_multiple_input_elements(self, handler):
        """Should process all input elements."""
        config = ApplyRegexConfig(
            field_name=NonEmptyStr("text"),
            expression=re.compile(r"\d+"),
            default_value=None,
            return_empty_result=False
        )
        input_list = [
            {"text": "a1", "id": 1},
            {"text": "b2", "id": 2},
            {"text": "no digits", "id": 3}  # Will be dropped
        ]

        result = await handler.handle(config, input_list)

        assert type(result) is CompletedWith.Data
        assert result.data == [
            {"text": "1", "id": 1},
            {"text": "2", "id": 2}
        ]
