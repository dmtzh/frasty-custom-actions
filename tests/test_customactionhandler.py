"""Unit tests for the ``customactionhandler`` abstraction layer.

These tests target the adaptation seam of the module: how
``to_action_handler_factory_input`` unpacks ``ActionData`` and adapts the
signatures of the two handler templates (with and without configuration),
plus the ``named_action_handler`` decorator and the abstract port contract.

The asynchronous ``handle`` coroutines are driven to completion with
``asyncio.run`` inside synchronous test bodies: ``pytest-asyncio`` is not part
of the project's dependencies, so no async test plugin is used.
"""

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest
from expression import Result

from customactionhandler import (
    ActionHandlerFactoryInput,
    CustomActionHandler,
    CustomActionHandlerWithoutConfig,
    RegistrableCustomActionHandler,
    named_action_handler,
)
from shared.action import Action, ActionName, ActionType
from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import Metadata, RunIdValue, StepIdValue
from shared.pipeline.actionhandler import ActionData, DataDto


# ======================================================================
# Test doubles — recording handlers used to observe delegation
# ======================================================================

@dataclass(frozen=True)
class _FakeConfig:
    prefix: str


class _RecordingWithConfigHandler(CustomActionHandler[_FakeConfig, list[DataDto]]):
    """Concrete ``CustomActionHandler`` that records every call it receives."""

    def __init__(self) -> None:
        self.handle_calls: list[tuple[_FakeConfig, list[DataDto]]] = []
        self.validate_input_calls: list[tuple[_FakeConfig, list[DataDto]]] = []

    @property
    def action_name(self) -> ActionName:
        return ActionName("recordingwithconfig")

    def validate_config(self, raw_config: dict[str, Any]) -> Result[_FakeConfig, Any]:
        prefix = raw_config.get("prefix")
        if isinstance(prefix, str) and prefix:
            return Result.Ok(_FakeConfig(prefix))
        return Result.Error("prefix is missing or empty")

    def validate_input(
        self, config: _FakeConfig, dto_list: list[DataDto]
    ) -> Result[list[DataDto], Any]:
        self.validate_input_calls.append((config, dto_list))
        return Result.Ok(dto_list)

    async def handle(self, config: _FakeConfig, input: list[DataDto]) -> CompletedResult:
        self.handle_calls.append((config, input))
        return CompletedWith.Data([{"prefix": config.prefix, "item": item} for item in input])


class _RecordingWithoutConfigHandler(CustomActionHandlerWithoutConfig[list[DataDto]]):
    """Concrete ``CustomActionHandlerWithoutConfig`` that records every call."""

    def __init__(self) -> None:
        self.handle_calls: list[list[DataDto]] = []
        self.validate_input_calls: list[list[DataDto]] = []

    @property
    def action_name(self) -> ActionName:
        return ActionName("recordingwithoutconfig")

    def validate_input(self, dto_list: list[DataDto]) -> Result[list[DataDto], Any]:
        self.validate_input_calls.append(dto_list)
        return Result.Ok(dto_list)

    async def handle(self, input: list[DataDto]) -> CompletedResult:
        self.handle_calls.append(input)
        return CompletedWith.Data([{"echo": item} for item in input])


# ======================================================================
# Shared fixtures
# ======================================================================

@pytest.fixture
def run():
    """Run a coroutine to completion from within a synchronous test body."""
    def _run(coro):
        return asyncio.run(coro)
    return _run


@pytest.fixture
def make_action_data():
    """Build an ``ActionData`` record with plausible run/step identifiers."""
    def _make(config: Any, input: Any) -> ActionData[Any, Any]:
        return ActionData(
            RunIdValue("runid01"),
            StepIdValue("stepid01"),
            config,
            input,
            Metadata({}),
        )
    return _make


# ======================================================================
# named_action_handler
# ======================================================================

class TestNamedActionHandler:
    def test_sets_handler_name_from_action_name(self):
        # Arrange
        @named_action_handler(ActionName("foo"))
        async def some_function(data):
            return None
        # Act
        name = some_function.__name__
        # Assert
        assert name == "handle_foo"

    def test_returns_the_same_function_object(self):
        # Arrange
        async def some_function(data):
            return None
        # Act
        decorated = named_action_handler(ActionName("foo"))(some_function)
        # Assert — the decorator must not wrap or replace the function.
        assert decorated is some_function


# ======================================================================
# RegistrableCustomActionHandler
# ======================================================================

class TestRegistrableCustomActionHandler:
    def test_abstract_class_cannot_be_instantiated(self):
        # Act + Assert — the port defines a single abstract method.
        with pytest.raises(TypeError):
            RegistrableCustomActionHandler() # pyright: ignore[reportAbstractUsage]

    def test_single_method_implementation_is_instantiable(self):
        async def handler_func(data):
            return None
        # Arrange — the port contract is exactly one method.
        class _MinimalHandler(RegistrableCustomActionHandler[str, list[DataDto]]):
            def to_action_handler_factory_input(self) -> ActionHandlerFactoryInput[str, list[DataDto]]:
                return ActionHandlerFactoryInput(
                    action=Action(ActionName("minimal"), ActionType.CUSTOM),
                    config_validator=lambda raw: Result.Ok(str(raw.get("value", ""))),
                    input_validator=lambda config, dto_list: Result.Ok(dto_list),
                    handler=handler_func
                )
        # Act
        handler = _MinimalHandler()
        factory_input = handler.to_action_handler_factory_input()
        # Assert
        assert isinstance(factory_input, ActionHandlerFactoryInput)
        assert factory_input.action == Action(ActionName("minimal"), ActionType.CUSTOM)


# ======================================================================
# CustomActionHandler — standard handler with configuration
# ======================================================================

class TestCustomActionHandler:
    @pytest.fixture
    def with_config_handler(self) -> _RecordingWithConfigHandler:
        return _RecordingWithConfigHandler()

    @pytest.mark.parametrize(
        "missing",
        ["action_name", "validate_config", "validate_input", "handle"],
    )
    def test_incomplete_subclass_cannot_be_instantiated(self, missing):
        # Arrange — a subclass that omits one of the four required members.
        class _Incomplete(CustomActionHandler[_FakeConfig, list[DataDto]]):
            if missing != "action_name":
                @property
                def action_name(self) -> ActionName:
                    return ActionName("incomplete")
            if missing != "validate_config":
                def validate_config(self, raw_config: dict[str, Any]) -> Result[_FakeConfig, Any]:
                    return Result.Ok(_FakeConfig("p"))
            if missing != "validate_input":
                def validate_input(
                    self, config: _FakeConfig, dto_list: list[DataDto]
                ) -> Result[list[DataDto], Any]:
                    return Result.Ok(dto_list)
            if missing != "handle":
                async def handle(self, config: _FakeConfig, input: list[DataDto]) -> CompletedResult:
                    return CompletedWith.NoData()
        # Act + Assert
        with pytest.raises(TypeError):
            _Incomplete()

    def test_factory_input_action_is_always_custom(self, with_config_handler):
        # Act
        factory_input = with_config_handler.to_action_handler_factory_input()
        # Assert — the action type is fixed by the template, not by the author.
        assert isinstance(factory_input, ActionHandlerFactoryInput)
        assert factory_input.action == Action(ActionName("recordingwithconfig"), ActionType.CUSTOM)

    def test_config_validator_delegates_to_validate_config(self, with_config_handler):
        # Arrange
        factory_input = with_config_handler.to_action_handler_factory_input()
        # Act
        result = factory_input.config_validator({"prefix": "pre"})
        # Assert
        assert result.is_ok()
        assert result.ok == _FakeConfig("pre")

    def test_config_validator_propagates_validation_error(self, with_config_handler):
        # Arrange
        factory_input = with_config_handler.to_action_handler_factory_input()
        # Act
        result = factory_input.config_validator({})
        # Assert
        assert result.is_error()
        assert result.error == "prefix is missing or empty"



    def test_input_validator_forwards_config_and_dto_list(self, with_config_handler):
        # Arrange
        factory_input = with_config_handler.to_action_handler_factory_input()
        config = _FakeConfig("pre")
        dto_list = [{"a": 1}]
        # Act
        result = factory_input.input_validator(config, dto_list)
        # Assert — both arguments reach validate_input in order.
        assert result.is_ok()
        assert result.ok == dto_list
        assert with_config_handler.validate_input_calls == [(config, dto_list)]

    def test_handler_name_reflects_action_name(self, with_config_handler):
        # Arrange
        factory_input = with_config_handler.to_action_handler_factory_input()
        # Act
        name = factory_input.handler.__name__
        # Assert
        assert name == "handle_recordingwithconfig"

    def test_handler_unpacks_action_data_into_config_and_input(
        self, with_config_handler, make_action_data, run
    ):
        # Arrange
        factory_input = with_config_handler.to_action_handler_factory_input()
        config = _FakeConfig("pre")
        dto_list = [{"a": 1}, {"b": 2}]
        action_data = make_action_data(config=config, input=dto_list)
        # Act
        run(factory_input.handler(action_data))
        # Assert — handle receives exactly the two values from ActionData.
        assert with_config_handler.handle_calls == [(config, dto_list)]

    def test_handler_returns_a_coroutine_before_await(
        self, with_config_handler, make_action_data, run
    ):
        # Arrange
        factory_input = with_config_handler.to_action_handler_factory_input()
        action_data = make_action_data(config=_FakeConfig("pre"), input=[])
        # Act
        coro = factory_input.handler(action_data)
        # Assert — the wrapper is a sync function that returns a coroutine.
        assert asyncio.iscoroutine(coro)
        run(coro)

    def test_handler_returns_the_completed_result(
        self, with_config_handler, make_action_data, run
    ):
        # Arrange
        factory_input = with_config_handler.to_action_handler_factory_input()
        action_data = make_action_data(config=_FakeConfig("pre"), input=[{"a": 1}])
        # Act
        completed = run(factory_input.handler(action_data))
        # Assert
        assert completed == CompletedWith.Data([{"prefix": "pre", "item": {"a": 1}}])


# ======================================================================
# CustomActionHandlerWithoutConfig — handler without configuration
# ======================================================================

class TestCustomActionHandlerWithoutConfig:
    @pytest.fixture
    def without_config_handler(self) -> _RecordingWithoutConfigHandler:
        return _RecordingWithoutConfigHandler()

    @pytest.mark.parametrize(
        "missing",
        ["action_name", "validate_input", "handle"],
    )
    def test_incomplete_subclass_cannot_be_instantiated(self, missing):
        # Arrange — a subclass that omits one of the three required members.
        class _Incomplete(CustomActionHandlerWithoutConfig[list[DataDto]]):
            if missing != "action_name":
                @property
                def action_name(self) -> ActionName:
                    return ActionName("incomplete")
            if missing != "validate_input":
                def validate_input(self, dto_list: list[DataDto]) -> Result[list[DataDto], Any]:
                    return Result.Ok(dto_list)
            if missing != "handle":
                async def handle(self, input: list[DataDto]) -> CompletedResult:
                    return CompletedWith.NoData()
        # Act + Assert
        with pytest.raises(TypeError):
            _Incomplete()

    def test_factory_input_action_is_always_custom(self, without_config_handler):
        # Act
        factory_input = without_config_handler.to_action_handler_factory_input()
        # Assert
        assert isinstance(factory_input, ActionHandlerFactoryInput)
        assert factory_input.action == Action(ActionName("recordingwithoutconfig"), ActionType.CUSTOM)

    @pytest.mark.parametrize("raw_config", [{}, {"a": 1}, "not-a-dict", None, 42])
    def test_config_validator_is_always_ok_none(self, without_config_handler, raw_config):
        # Arrange
        factory_input = without_config_handler.to_action_handler_factory_input()
        # Act
        result = factory_input.config_validator(raw_config)
        # Assert — there is no config, so any raw value is accepted as None.
        assert result.is_ok()
        assert result.ok is None

    def test_input_validator_forwards_only_the_dto_list(self, without_config_handler):
        # Arrange
        factory_input = without_config_handler.to_action_handler_factory_input()
        dto_list = [{"a": 1}]
        # Act
        result = factory_input.input_validator("ignored-config-sentinel", dto_list)
        # Assert — the config argument is dropped (arity adaptation).
        assert result.is_ok()
        assert result.ok == dto_list
        assert without_config_handler.validate_input_calls == [dto_list]

    def test_handler_name_reflects_action_name(self, without_config_handler):
        # Arrange
        factory_input = without_config_handler.to_action_handler_factory_input()
        # Act
        name = factory_input.handler.__name__
        # Assert
        assert name == "handle_recordingwithoutconfig"

    def test_handler_forwards_only_the_input(
        self, without_config_handler, make_action_data, run
    ):
        # Arrange
        factory_input = without_config_handler.to_action_handler_factory_input()
        dto_list = [{"a": 1}, {"b": 2}]
        action_data = make_action_data(config=None, input=dto_list)
        # Act
        run(factory_input.handler(action_data))
        # Assert — handle receives exactly one argument (the input only).
        assert without_config_handler.handle_calls == [dto_list]

    def test_handler_returns_the_completed_result(
        self, without_config_handler, make_action_data, run
    ):
        # Arrange
        factory_input = without_config_handler.to_action_handler_factory_input()
        action_data = make_action_data(config=None, input=[{"a": 1}])
        # Act
        completed = run(factory_input.handler(action_data))
        # Assert
        assert completed == CompletedWith.Data([{"echo": {"a": 1}}])

