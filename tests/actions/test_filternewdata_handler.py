import pytest
from unittest.mock import MagicMock

from actions.filternewdata.handler import (
    FilterNewDataHandler,
    FilterNewDataConfig,
    CompareTo,
    ProjectedDataDto,
)
from actions.filternewdata.previousdatastore import PreviousDataStore
from actions.filternewdata.projection import create_projection_resolver
from shared.pipeline.actionhandler import DataDto
from shared.utils.parse import NonEmptyStr

@pytest.fixture
def mock_previous_data_store() -> MagicMock:
    """Create a mock PreviousDataStore for handler tests."""
    store = MagicMock(spec=PreviousDataStore)
    return store

@pytest.fixture
def handler(mock_previous_data_store: MagicMock) -> FilterNewDataHandler:
    """Create a FilterNewDataHandler with mocked storage."""
    return FilterNewDataHandler(mock_previous_data_store)


class TestFilterNewDataHandlerValidateConfig:
    """Tests for FilterNewDataHandler.validate_config."""

    def test_valid_config_returns_ok(self, handler: FilterNewDataHandler):
        """Valid config returns Result.Ok with FilterNewDataConfig."""
        raw_config = {"set_name": "my_set"}
        result = handler.validate_config(raw_config)
        
        assert result.is_ok()
        config = result.ok
        assert isinstance(config, FilterNewDataConfig)
        assert config.set_name == "my_set"
        assert config.compare_to == CompareTo.ALL

    def test_invalid_config_returns_formatted_error(self, handler: FilterNewDataHandler):
        """Invalid config returns formatted error via format_value_errors."""
        raw_config = {"set_name": ""}
        result = handler.validate_config(raw_config)
        
        assert result.is_error()
        # format_value_errors transforms tuple[ValueErr, ...] into formatted representation
        error = result.error
        assert error is not None

    def test_multiple_errors_are_formatted(self, handler: FilterNewDataHandler):
        """Multiple errors are accumulated and formatted."""
        raw_config = {"set_name": "", "compare_to": "UNKNOWN"}
        result = handler.validate_config(raw_config)
        
        assert result.is_error()
        error = result.error
        assert error is not None


class TestFilterNewDataHandlerValidateInput:
    """Tests for FilterNewDataHandler.validate_input."""

    @pytest.fixture
    def identity_config(self) -> FilterNewDataConfig:
        """Config with identity projection (no projection)."""
        resolver = create_projection_resolver(None).ok
        return FilterNewDataConfig(
            set_name=NonEmptyStr("my_set"),
            compare_to=CompareTo.ALL,
            resolver=resolver,
        )

    @pytest.fixture
    def projection_config(self) -> FilterNewDataConfig:
        """Config with projection on 'user.id'."""
        resolver = create_projection_resolver(NonEmptyStr("user.id")).ok
        return FilterNewDataConfig(
            set_name=NonEmptyStr("my_set"),
            compare_to=CompareTo.ALL,
            resolver=resolver,
        )

    def test_valid_input_with_identity_projection(self, handler: FilterNewDataHandler, identity_config: FilterNewDataConfig):
        """Valid input with identity projection returns Ok."""
        dto_list: list[DataDto] = [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
        ]
        result = handler.validate_input(identity_config, dto_list)
        
        assert result.is_ok()
        projected_input = result.ok
        assert len(projected_input) == 2
        assert all(isinstance(item, ProjectedDataDto) for item in projected_input)
        # Identity projection: dto == projection
        assert projected_input[0].dto == projected_input[0].projection
        assert projected_input[1].dto == projected_input[1].projection

    def test_valid_input_with_projection(self, handler: FilterNewDataHandler, projection_config: FilterNewDataConfig):
        """Valid input with projection returns Ok with projected data."""
        dto_list: list[DataDto] = [
            {"id": "1", "user": {"id": "u1"}},
            {"id": "2", "user": {"id": "u2"}},
        ]
        result = handler.validate_input(projection_config, dto_list)
        
        assert result.is_ok()
        projected_input = result.ok
        assert len(projected_input) == 2
        # Projection wraps scalar in {"value": ...}
        assert projected_input[0].projection == {"value": "u1"}
        assert projected_input[1].projection == {"value": "u2"}

    def test_invalid_input_projection_error(self, handler: FilterNewDataHandler, projection_config: FilterNewDataConfig):
        """Missing projection field returns ValueInvalid with index."""
        dto_list: list[DataDto] = [
            {"id": "1"},  # Missing 'user' field
        ]
        result = handler.validate_input(projection_config, dto_list)
        
        assert result.is_error()
        error = result.error
        assert error is not None

    def test_multiple_projection_errors_accumulated(self, handler: FilterNewDataHandler, projection_config: FilterNewDataConfig):
        """Multiple projection errors are accumulated with indices."""
        dto_list: list[DataDto] = [
            {"id": "1"},  # Missing 'user' field
            {"id": "2"},  # Missing 'user' field
        ]
        result = handler.validate_input(projection_config, dto_list)
        
        assert result.is_error()
        error = result.error
        assert error is not None

    def test_empty_input_returns_ok(self, handler: FilterNewDataHandler, identity_config: FilterNewDataConfig):
        """Empty input list returns Ok with empty list."""
        dto_list: list[DataDto] = []
        result = handler.validate_input(identity_config, dto_list)
        
        assert result.is_ok()
        assert result.ok == []

    def test_mixed_valid_and_invalid_input(self, handler: FilterNewDataHandler, projection_config: FilterNewDataConfig):
        """Mixed valid and invalid input accumulates errors only."""
        dto_list: list[DataDto] = [
            {"id": "1", "user": {"id": "u1"}},  # Valid
            {"id": "2"},  # Invalid: missing 'user' field
        ]
        result = handler.validate_input(projection_config, dto_list)
        
        assert result.is_error()
        error = result.error
        assert error is not None
