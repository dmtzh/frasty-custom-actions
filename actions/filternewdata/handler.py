from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Optional

from expression import Result

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.infrastructure.storage.repository import StorageError
from shared.pipeline.actionhandler import DataDto
from shared.utils.asyncresult import async_ex_to_error_result
from shared.utils.parse import parse_from_dict, parse_non_empty_str
from shared.utils.string import strip_and_lowercase

from customactionhandler import CustomActionHandler

from .previousdatastore import previous_data_storage

class CompareTo(StrEnum):
    ALL = "ALL"
    LAST = "LAST"

    @staticmethod
    def parse(compare_to: str) -> Optional['CompareTo']:
        if compare_to is None:
            return None
        match strip_and_lowercase(compare_to):
            case "all":
                return CompareTo.ALL
            case "last":
                return CompareTo.LAST
            case _:
                return None

@dataclass(frozen=True)
class FilterNewDataConfig:
    set_name: str
    compare_to: CompareTo

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['FilterNewDataConfig', str]:
        def validate_set_name() -> Result[str, str]:
            return parse_from_dict(data, "set_name", parse_non_empty_str)
        def validate_compare_to() -> Result[CompareTo, str]:
            if "compare_to" in data:
                return parse_from_dict(data, "compare_to", CompareTo.parse)
            else:
                return Result.Ok(CompareTo.ALL)
        set_name_res = validate_set_name()
        compare_to_res = validate_compare_to()
        errs = [err for err in [set_name_res.swap().default_value(None), compare_to_res.swap().default_value(None)] if err is not None]
        match errs:
            case []:
                return Result.Ok(FilterNewDataConfig(set_name_res.ok, compare_to_res.ok))
            case _:
                return Result.Error(", ".join(errs))

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
        @previous_data_storage.with_storage
        def apply_append_to_all(items: dict[str, FilterNewDataInput] | None, input: FilterNewDataInput):
            match items:
                case None:
                    return (input, {CompareTo.ALL.value: input})
                case _:
                    if CompareTo.ALL not in items:
                        return (input, items | {CompareTo.ALL: input})
                    new_items = [item for item in input if item not in items[CompareTo.ALL]]
                    items[CompareTo.ALL] = items[CompareTo.ALL] + new_items
                    return (new_items, items)
        @async_ex_to_error_result(StorageError.from_exception)
        @previous_data_storage.with_storage
        def apply_replace_last(items: dict[str, FilterNewDataInput] | None, input: FilterNewDataInput):
            match items:
                case None:
                    return (input, {CompareTo.LAST.value: input})
                case _:
                    if CompareTo.LAST not in items:
                        return (input, items | {CompareTo.LAST: input})
                    new_items = [item for item in input if item not in items[CompareTo.LAST]]
                    items[CompareTo.LAST] = input
                    return (new_items, items)
        def ok_to_completed_result(result_data: list[DataDto]):
            return CompletedWith.Data(result_data) if result_data else CompletedWith.NoData()
        def err_to_completed_result(err):
            return CompletedWith.Error(str(err))
        
        match config.compare_to:
            case CompareTo.ALL:
                new_data_res = await apply_append_to_all(config.set_name, input)
            case CompareTo.LAST:
                new_data_res = await apply_replace_last(config.set_name, input)
        completed_result = new_data_res.map(ok_to_completed_result).default_with(err_to_completed_result)
        return completed_result
