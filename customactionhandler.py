from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, override

from expression import Result

from shared.action import Action, ActionName, ActionType
from shared.completedresult import CompletedResult
from shared.pipeline.actionhandler import ActionData, ActionHandlerFactory, AsyncActionHandler, DataDto, RunAsyncAction

TCfg = TypeVar("TCfg")
D = TypeVar("D")

def named_action_handler(action_name: ActionName):
    def decorator(handler: Callable[[ActionData[TCfg, D]], Coroutine[Any, Any, CompletedResult | None]]):
        handler.__name__ = f"handle_{action_name}"
        return handler
    return decorator

@dataclass(frozen=True)
class ActionHandlerFactoryInput(Generic[TCfg, D]):
    action: Action
    config_validator: Callable[[dict[str, Any]], Result[TCfg, Any]]
    input_validator: Callable[[TCfg, list[DataDto]], Result[D, Any]]
    handler: Callable[[ActionData[TCfg, D]], Coroutine[Any, Any, CompletedResult | None]]


class RegistrableCustomActionHandler(ABC, Generic[TCfg, D]):
    """
    Base class for all custom action handlers that can be registered
    with the ActionHandlerFactory.

    This class defines the registration contract
    
    """

    # ------------------------------------------------------------------
    # Abstract contract
    # ------------------------------------------------------------------

    @abstractmethod
    def to_action_handler_factory_input(self) -> ActionHandlerFactoryInput[TCfg, D]:
        raise NotImplementedError()


# ======================================================================
# CustomActionHandler — standard handler with configuration
# ======================================================================

class CustomActionHandler(RegistrableCustomActionHandler[TCfg, D]):
    """
    Standard custom action handler with configuration.

    Subclasses must implement:
      - ``action_name`` (property)
      - ``validate_config``
      - ``validate_input``
      - ``handle``
    """
    @property
    @abstractmethod
    def action_name(self) -> ActionName:
        raise NotImplementedError()
    
    @abstractmethod
    def validate_config(self, raw_config: dict[str, Any]) -> Result[TCfg, Any]:
        raise NotImplementedError()
        
    @abstractmethod
    def validate_input(self, config: TCfg, dto_list: list[DataDto]) -> Result[D, Any]:
        raise NotImplementedError()

    @abstractmethod
    async def handle(self, config: TCfg, input: D) -> CompletedResult:
        raise NotImplementedError()

    @override
    def to_action_handler_factory_input(self) -> ActionHandlerFactoryInput[TCfg, D]:
        @named_action_handler(self.action_name)
        def handle_wrapper(data: ActionData[TCfg, D]):
            """Adapts to the ``handle(config, input)`` signature."""
            return self.handle(data.config, data.input)
        return ActionHandlerFactoryInput(
            action=Action(self.action_name, ActionType.CUSTOM),
            config_validator=self.validate_config,
            input_validator=self.validate_input,
            handler=handle_wrapper
        )


# ======================================================================
# CustomActionHandlerWithoutConfig — handler without configuration
# ======================================================================

class CustomActionHandlerWithoutConfig(RegistrableCustomActionHandler[None, D]):
    """
    Custom action handler without configuration.

    Subclasses must implement:
      - ``action_name`` (property)
      - ``validate_input``
      - ``handle``
    """
    @property
    @abstractmethod
    def action_name(self) -> ActionName:
        raise NotImplementedError()
    
    @abstractmethod
    def validate_input(self, dto_list: list[DataDto]) -> Result[D, Any]:
        raise NotImplementedError()
    
    @abstractmethod
    async def handle(self, input: D) -> CompletedResult:
        raise NotImplementedError()

    @override
    def to_action_handler_factory_input(self) -> ActionHandlerFactoryInput[None, D]:
        def validate_input_wrapper(_: None, dto_list: list[DataDto]):
            """Adapts to the ``validate_input(input)`` signature — no config."""
            return self.validate_input(dto_list)
        @named_action_handler(self.action_name)
        def handle_wrapper(data: ActionData[None, D]):
            """Adapts to the ``handle(input)`` signature — no config."""
            return self.handle(data.input)
        return ActionHandlerFactoryInput(
            action=Action(self.action_name, ActionType.CUSTOM),
            config_validator=lambda _: Result.Ok(None),
            input_validator=validate_input_wrapper,
            handler=handle_wrapper
        )

def create_custom_action_registration_handler(run_action: RunAsyncAction, action_handler: AsyncActionHandler):
    def register_custom_action(custom_action: RegistrableCustomActionHandler[TCfg, D]):
        factory_input = custom_action.to_action_handler_factory_input()
        return ActionHandlerFactory(run_action, action_handler).create(
                factory_input.action,
                factory_input.config_validator,
                factory_input.input_validator,
            )(factory_input.handler)
    return register_custom_action
