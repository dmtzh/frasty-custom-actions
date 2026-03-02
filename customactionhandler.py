from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from expression import Result

from shared.action import Action, ActionName, ActionType
from shared.completedresult import CompletedResult
from shared.pipeline.actionhandler import ActionData, ActionHandlerFactory, AsyncActionHandler, DataDto, RunAsyncAction

TCfg = TypeVar("TCfg")
D = TypeVar("D")

class CustomActionHandler(ABC, Generic[TCfg, D]):
    @property
    @abstractmethod
    def action_name(self) -> ActionName:
        raise NotImplementedError()
    
    @property
    def action(self) -> Action:
        return Action(self.action_name, ActionType.CUSTOM)
    
    @abstractmethod
    def validate_config(self, raw_config: dict[str, Any]) -> Result[TCfg, Any]:
        raise NotImplementedError()
        
    @abstractmethod
    def validate_input(self, dto_list: list[DataDto]) -> Result[D, Any]:
        raise NotImplementedError()

    @abstractmethod
    async def handle(self, config: TCfg, input: D) -> CompletedResult:
        raise NotImplementedError()

class CustomActionHandlerWithoutConfig(CustomActionHandler[None, D]):
    def validate_config(self, raw_config: dict[str, Any]) -> Result[None, Any]:
        return Result.Ok(None)
    
    @abstractmethod
    async def handle(self, input: D) -> CompletedResult:
        raise NotImplementedError()

def create_custom_action_registration_handler(run_action: RunAsyncAction, action_handler: AsyncActionHandler):
    def register_custom_action[TCfg, D](handler: CustomActionHandler[TCfg, D]):
        def handle_wrapper(data: ActionData[TCfg, D]):
            return handler.handle(data.input) if isinstance(handler, CustomActionHandlerWithoutConfig) else handler.handle(data.config, data.input)
        handle_wrapper.__name__ = f"handle_{handler.action.name}"
        return ActionHandlerFactory(run_action, action_handler).create(
            handler.action,
            handler.validate_config,
            handler.validate_input
        )(handle_wrapper)
    return register_custom_action
