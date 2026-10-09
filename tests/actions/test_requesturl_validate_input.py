import pytest
from expression import Result

from actions.requesturl import RequestUrlConfig, RequestUrlHandler
from shared.pipeline.actionhandler import DataDto


class TestRequestUrlValidateInputMissingJson:
    @pytest.fixture
    def handler(self) -> RequestUrlHandler:
        return RequestUrlHandler()

    @pytest.fixture
    def config(self) -> RequestUrlConfig:
        return RequestUrlConfig(delay_between_requests=0)

    def test_missing_json_is_valid(self, handler: RequestUrlHandler, config: RequestUrlConfig):
        # Arrange a minimal valid input DTO without the 'json' key at all.
        dto: DataDto = {
            "url": "https://example.com",
            "http_method": "GET",
        }

        # Act
        result = handler.validate_input(config, [dto])

        # Assert
        match result:
            case Result(tag="ok", ok=validated):
                assert len(validated) == 1
                assert validated[0].json is None

            case Result(tag="error", error=errors):
                pytest.fail(f"Expected valid input when json is missing, got errors: {errors}")

            case _:
                pytest.fail("Unexpected Result shape")

    def test_missing_json_and_missing_headers_is_valid(
        self, handler: RequestUrlHandler, config: RequestUrlConfig
    ):
        # Arrange a bare request DTO: neither 'json' nor 'headers' is present.
        dto: DataDto = {
            "url": "https://example.com",
            "http_method": "POST",
        }

        # Act
        result = handler.validate_input(config, [dto])

        # Assert
        match result:
            case Result(tag="ok", ok=validated):
                assert len(validated) == 1
                assert validated[0].json is None
                assert validated[0].headers is None

            case Result(tag="error", error=errors):
                pytest.fail(f"Expected valid input when json is missing, got errors: {errors}")

            case _:
                pytest.fail("Unexpected Result shape")


class TestRequestUrlValidateInputJsonRegression:
    """Behaviour that must stay unchanged while fixing the missing 'json' key case."""

    @pytest.fixture
    def handler(self) -> RequestUrlHandler:
        return RequestUrlHandler()

    @pytest.fixture
    def config(self) -> RequestUrlConfig:
        return RequestUrlConfig(delay_between_requests=0)

    @staticmethod
    def _validate(handler: RequestUrlHandler, config: RequestUrlConfig, dto: DataDto):
        return handler.validate_input(config, [dto])

    def test_empty_json_object_is_valid(self, handler: RequestUrlHandler, config: RequestUrlConfig):
        # Arrange
        dto: DataDto = {"url": "https://example.com", "http_method": "GET", "json": {}}

        # Act
        result = self._validate(handler, config, dto)

        # Assert
        assert result.is_ok()
        assert result.ok[0].json == {}

    def test_valid_json_object_is_valid(self, handler: RequestUrlHandler, config: RequestUrlConfig):
        # Arrange
        dto: DataDto = {"url": "https://example.com", "http_method": "GET", "json": {"key": "value"}}

        # Act
        result = self._validate(handler, config, dto)

        # Assert
        assert result.is_ok()
        assert result.ok[0].json == {"key": "value"}

    def test_invalid_json_value_is_error(self, handler: RequestUrlHandler, config: RequestUrlConfig):
        # Arrange: a non-dict 'json' value is not a valid request body.
        dto: DataDto = {"url": "https://example.com", "http_method": "GET", "json": "invalid"}

        # Act
        result = self._validate(handler, config, dto)

        # Assert
        assert result.is_error()

    def test_explicit_json_null_is_error(self, handler: RequestUrlHandler, config: RequestUrlConfig):
        # An explicit null is intentionally left out of scope for this fix and
        # must keep behaving exactly as before (a validation error).
        dto: DataDto = {"url": "https://example.com", "http_method": "GET", "json": None}

        result = self._validate(handler, config, dto)

        assert result.is_error()

    def test_present_headers_still_validated(self, handler: RequestUrlHandler, config: RequestUrlConfig):
        # Arrange: the optional 'headers' key keeps working when it is provided.
        dto: DataDto = {"url": "https://example.com", "http_method": "GET", "headers": {"a": "b"}}

        result = self._validate(handler, config, dto)

        assert result.is_ok()
        assert result.ok[0].headers == {"a": "b"}

    def test_missing_url_is_error(self, handler: RequestUrlHandler, config: RequestUrlConfig):
        # Arrange: a required field is still reported as an error.
        dto: DataDto = {"http_method": "GET"}

        result = self._validate(handler, config, dto)

        assert result.is_error()

    def test_empty_input_list_is_error(self, handler: RequestUrlHandler, config: RequestUrlConfig):
        result = handler.validate_input(config, [])

        assert result.is_error()
