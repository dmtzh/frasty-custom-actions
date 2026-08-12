from dataclasses import dataclass
from typing import Any

from expression import Result

from shared.pipeline.actionhandler import DataDto
from shared.utils.parse import parse_from_dict, parse_value

@dataclass(frozen=True)
class InputToActionsConfig:
    actions: list[DataDto]
    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['InputToActionsConfig', str]:
        def validate_actions() -> Result[list[DataDto], str]:
            actions_list_res = parse_from_dict(data, "actions", lambda lst: lst if isinstance(lst, list) and lst else None)
            actions_list_of_dict_res = actions_list_res.bind(lambda actions: parse_value(actions, "actions", lambda actions: actions if all(isinstance(action, dict) and action for action in actions) else None))
            return actions_list_of_dict_res
        actions_res = validate_actions()
        return actions_res.map(InputToActionsConfig)
