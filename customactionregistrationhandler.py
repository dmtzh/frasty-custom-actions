from typing import TypeVar

from shared.pipeline.actionhandler import ActionHandlerFactory, AsyncActionHandler, RunAsyncAction

from customactionhandler import RegistrableCustomActionHandler

TCfg = TypeVar("TCfg")
D = TypeVar("D")

def create_custom_action_registration_handler(run_action: RunAsyncAction, action_handler: AsyncActionHandler):
    def register_custom_action(custom_action: RegistrableCustomActionHandler[TCfg, D]):
        factory_input = custom_action.to_action_handler_factory_input()
        return ActionHandlerFactory(run_action, action_handler).create(
                factory_input.action,
                factory_input.config_validator,
                factory_input.input_validator,
            )(factory_input.handler)
    return register_custom_action