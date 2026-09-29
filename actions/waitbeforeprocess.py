import asyncio
from dataclasses import dataclass
from typing import Any

from expression import Result

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.pipeline.actionhandler import DataDto
from shared.utils.parse import PositiveInt, parse_dict_field
from shared.validation import format_value_errors, ValueError as ValueErr

from customactionhandler import CustomActionHandler

@dataclass(frozen=True)
class WaitBeforeProcessConfig:
    duration_ms: PositiveInt

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result["WaitBeforeProcessConfig", tuple[ValueErr, ...]]:
        duration_ms_res = parse_dict_field(data, "duration_ms", PositiveInt.parse)
        return duration_ms_res.map(WaitBeforeProcessConfig).map_error(lambda err: tuple([err]))

type WaitBeforeProcessInput = list[DataDto]

class WaitBeforeProcessHandler(CustomActionHandler[WaitBeforeProcessConfig, WaitBeforeProcessInput]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("waitbeforeprocess")

    def validate_config(self, raw_config: dict[str, Any]) -> Result[WaitBeforeProcessConfig, Any]:
        return WaitBeforeProcessConfig.from_dict(raw_config).map_error(format_value_errors)
    
    def validate_input(self, _: WaitBeforeProcessConfig, dto_list: list[DataDto]) -> Result[WaitBeforeProcessInput, Any]:
        return Result.Ok(dto_list)
    
    async def handle(self, config: WaitBeforeProcessConfig, input_list: WaitBeforeProcessInput) -> CompletedResult:
        await asyncio.sleep(config.duration_ms / 1000)
        return CompletedWith.Data(input_list)
