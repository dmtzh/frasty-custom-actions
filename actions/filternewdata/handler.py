from dataclasses import dataclass
from typing import Any

from expression import Result

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.pipeline.actionhandler import DataDto
from shared.utils.parse import parse_non_empty_str

from customactionhandler import CustomActionHandler

@dataclass(frozen=True)
class FilterNewDataConfig:
    set_name: str

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['FilterNewDataConfig', str]:
        def validate_set_name() -> Result[str, str]:
            if "set_name" in data:
                return parse_non_empty_str(data["set_name"], "set_name")
            else:
                return Result.Error("'set_name' key is missing")
        set_name_res = validate_set_name()
        return set_name_res.map(FilterNewDataConfig)

type FilterNewDataInput = list[DataDto]

class FilterNewDataHandler(CustomActionHandler[FilterNewDataConfig, FilterNewDataInput]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("filternewdata")
    
    def validate_config(self, raw_config: dict[str, Any]) -> Result[FilterNewDataConfig, Any]:
        return FilterNewDataConfig.from_dict(raw_config)
    
    def validate_input(self, dto_list: list[DataDto]) -> Result[FilterNewDataInput, Any]:
        return Result.Ok(dto_list)
    
    async def handle(self, config: FilterNewDataConfig, input: FilterNewDataInput) -> CompletedResult:
        return CompletedWith.Data(data=input)
