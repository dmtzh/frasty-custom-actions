import asyncio
from dataclasses import dataclass
from typing import Any

from expression import Result

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.pipeline.actionhandler import DataDto
from shared.utils.parse import PositiveInt, parse_from_dict

from customactionhandler import CustomActionHandler

@dataclass(frozen=True)
class WaitBeforeProcessConfig:
    duration_ms: PositiveInt

    @staticmethod
    def from_dict(data: dict[str, Any]):
        def validate_duration_ms():
            return parse_from_dict(data, "duration_ms", PositiveInt.parse)
        duration_ms_res = validate_duration_ms()
        return duration_ms_res.map(WaitBeforeProcessConfig)

type WaitBeforeProcessInput = list[DataDto]

class WaitBeforeProcessHandler(CustomActionHandler[WaitBeforeProcessConfig, WaitBeforeProcessInput]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("waitbeforeprocess")
    
    def validate_config(self, raw_config: dict[str, Any]) -> Result[WaitBeforeProcessConfig, Any]:
        return WaitBeforeProcessConfig.from_dict(raw_config)
    
    def validate_input(self, _: WaitBeforeProcessConfig, dto_list: list[DataDto]) -> Result[WaitBeforeProcessInput, Any]:
        return Result.Ok(dto_list)
    
    async def handle(self, config: WaitBeforeProcessConfig, input_list: WaitBeforeProcessInput) -> CompletedResult:
        await asyncio.sleep(config.duration_ms / 1000)
        return CompletedWith.Data(input_list)
