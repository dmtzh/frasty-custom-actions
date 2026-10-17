from dataclasses import dataclass
from typing import Any

from expression import Result

from shared.pipeline.actionhandler import DataDto
from shared.utils.parse import parse_dict_field
from shared.utils.result import traverse_accumulating_with_index
from shared.validation import (
    ValueError as ValueErr,
    ValueInvalid
)

@dataclass(frozen=True)
class InputToActionsConfig:
    actions: list[DataDto]
    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['InputToActionsConfig', tuple[ValueErr, ...]]:
        """
        Parse raw config dict into InputToActionsConfig.

        Returns Result.Error with structured ValueError tuple on failure,
        compatible with shared.validation.format_value_errors.
        """
        def _validate_action_item(
            idx: int, item: Any,
        ) -> Result[DataDto, ValueErr]:
            if isinstance(item, dict) and item:
                return Result.Ok(item)
            return Result.Error(ValueInvalid(f"actions[{idx}]", item))
        
        # 1. Extract 'actions' field as list
        actions_list_res = parse_dict_field(
            data,
            "actions",
            lambda v: v if isinstance(v, list) else None
        ).map_error(lambda err: tuple([err]))

        # 2. Validate each element with index-aware error accumulation
        validated_res = actions_list_res.bind(
            lambda actions: traverse_accumulating_with_index(
                actions, _validate_action_item
            )
        )

        return validated_res.map(InputToActionsConfig)
