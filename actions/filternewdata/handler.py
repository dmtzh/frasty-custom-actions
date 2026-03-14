from dataclasses import dataclass
from typing import Any

from expression import Result

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.infrastructure.storage.repository import StorageError
from shared.pipeline.actionhandler import DataDto
from shared.utils.asyncresult import async_catch_ex, async_ex_to_error_result
from shared.utils.parse import parse_non_empty_str

from customactionhandler import CustomActionHandler
from .previousdatastore import previous_data_storage

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
        @async_ex_to_error_result(StorageError.from_exception)
        async def get_new_data(config: FilterNewDataConfig, data: FilterNewDataInput):
            if not data:
                return []
            opt_data_to_exclude = await previous_data_storage.get(config.set_name)
            match opt_data_to_exclude:
                case None:
                    return data
                case data_to_exclude:
                    filtered_data = [item for item in data if item not in data_to_exclude]
                    return filtered_data
        def ok_to_completed_result(result_data: list[DataDto]):
            return CompletedWith.Data(result_data) if result_data else CompletedWith.NoData()
        def err_to_completed_result(err):
            return CompletedWith.Error(str(err))
        
        new_data_res = await get_new_data(config, input)
        completed_result = new_data_res.map(ok_to_completed_result).default_with(err_to_completed_result)
        match completed_result:
            case CompletedWith.Data(new_data):
                await async_catch_ex(previous_data_storage.append)(config.set_name, new_data)
        return completed_result
