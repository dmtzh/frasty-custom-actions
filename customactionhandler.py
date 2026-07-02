from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar, override

from expression import Result

from shared.action import Action, ActionName, ActionType
from shared.completedresult import CompletedResult
from shared.pipeline.actionhandler import ActionData, ActionHandlerFactory, AsyncActionHandler, DataDto, RunAsyncAction

TCfg = TypeVar("TCfg")
D = TypeVar("D")

class RegistrableCustomActionHandler(ABC):
    @abstractmethod
    def register(self, run_action: RunAsyncAction, action_handler: AsyncActionHandler) -> Any:
        raise NotImplementedError()

class CustomActionHandler(RegistrableCustomActionHandler, Generic[TCfg, D]):
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
    def register(self, run_action: RunAsyncAction, action_handler: AsyncActionHandler) -> Any:
        def handle_wrapper(data: ActionData[TCfg, D]):
            return self.handle(data.config, data.input)
        action = Action(self.action_name, ActionType.CUSTOM)
        handle_wrapper.__name__ = f"handle_{action.name}"
        return ActionHandlerFactory(run_action, action_handler).create(
            action,
            self.validate_config,
            self.validate_input
        )(handle_wrapper)

class CustomActionHandlerWithoutConfig(RegistrableCustomActionHandler, Generic[D]):
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
    def register(self, run_action: RunAsyncAction, action_handler: AsyncActionHandler) -> Any:
        def handle_wrapper(data: ActionData[None, D]):
            return self.handle(data.input)
        action = Action(self.action_name, ActionType.CUSTOM)
        handle_wrapper.__name__ = f"handle_{action.name}"
        return ActionHandlerFactory(run_action, action_handler).create_without_config(
            action,
            self.validate_input
        )(handle_wrapper)

def create_custom_action_registration_handler(run_action: RunAsyncAction, action_handler: AsyncActionHandler):
    def register_custom_action(custom_action: RegistrableCustomActionHandler):
        return custom_action.register(run_action, action_handler)
    return register_custom_action
