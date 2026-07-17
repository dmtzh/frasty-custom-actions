from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Optional

from expression import Result

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.infrastructure.storage.repository import StorageError
from shared.pipeline.actionhandler import DataDto
from shared.utils.exceptiondecorators import async_ex_to_error_result
from shared.utils.parse import parse_from_dict, parse_non_empty_str
from shared.utils.result import to_error_list
from shared.utils.string import strip_and_lowercase

from config import previous_data_storage
from customactionhandler import CustomActionHandler

class CompareTo(StrEnum):
    ALL = "ALL"
    LAST = "LAST"
    ALL_READONLY = "ALL_READONLY"
    LAST_READONLY = "LAST_READONLY"

    @staticmethod
    def parse(compare_to: str) -> Optional['CompareTo']:
        if compare_to is None:
            return None
        match strip_and_lowercase(compare_to):
            case "all":
                return CompareTo.ALL
            case "last":
                return CompareTo.LAST
            case "all_readonly":
                return CompareTo.ALL_READONLY
            case "last_readonly":
                return CompareTo.LAST_READONLY
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
        errs = to_error_list(set_name_res, compare_to_res)
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
    
    def validate_input(self, _: FilterNewDataConfig, dto_list: list[DataDto]) -> Result[FilterNewDataInput, Any]:
        return Result.Ok(dto_list)
    
    async def handle(self, config: FilterNewDataConfig, input: FilterNewDataInput) -> CompletedResult:
        def ok_to_completed_result(result_data: list[DataDto]):
            return CompletedWith.Data(result_data) if result_data else CompletedWith.NoData()
        def err_to_completed_result(err):
            return CompletedWith.Error(str(err))
        
        # Get the strategy function from the dictionary
        strategy_func = _COMPARE_TO_STRATEGIES.get(config.compare_to)
        if strategy_func is None:
            return CompletedWith.Error(f"Invalid configuration: unknown compare_to mode '{config.compare_to}'")

        # Execute the strategy
        new_data_res = await strategy_func(config.set_name, input)
        completed_result = new_data_res.map(ok_to_completed_result).default_with(err_to_completed_result)
        return completed_result

# --- Module-level compare to strategy functions ---
type CompareToStrategyFunc = Callable[[str, FilterNewDataInput], Coroutine[Any, Any, Result[list[DataDto], StorageError]]]

@async_ex_to_error_result(StorageError.from_exception)
@previous_data_storage.with_storage
def _apply_append_to_all(items: dict[str, FilterNewDataInput] | None, input: FilterNewDataInput) -> tuple[list[DataDto], dict[str, FilterNewDataInput]]:
    """
    Accumulates all unique items seen so far for the given set_name.
    Returns new unique items and the updated state.
    """
    match items:
        case None:
            return (input, {CompareTo.ALL.value: input})
        case _:
            if CompareTo.ALL.value not in items:
                return (input, items | {CompareTo.ALL.value: input})
            
            history = items[CompareTo.ALL.value]
            new_items = [item for item in input if item not in history]
            
            # Update state with new unique items appended
            updated_history = history + new_items
            items[CompareTo.ALL.value] = updated_history
            
            return (new_items, items)


@async_ex_to_error_result(StorageError.from_exception)
@previous_data_storage.with_storage
def _apply_replace_last(items: dict[str, FilterNewDataInput] | None, input: FilterNewDataInput) -> tuple[list[DataDto], dict[str, FilterNewDataInput]]:
    """
    Compares input with the last batch of items seen for the given set_name.
    Updates the state to reflect the current input as the last batch.
    """
    match items:
        case None:
            return (input, {CompareTo.LAST.value: input})
        case _:
            if CompareTo.LAST.value not in items:
                return (input, items | {CompareTo.LAST.value: input})
            
            last_batch = items[CompareTo.LAST.value]
            new_items = [item for item in input if item not in last_batch]
            
            # Update state to current input
            items[CompareTo.LAST.value] = input
            
            return (new_items, items)


@async_ex_to_error_result(StorageError.from_exception)
async def _apply_readonly_filter_all(set_name: str, input: FilterNewDataInput) -> list[DataDto]:
    """
    Filters out items already present in the accumulated history (ALL mode) without updating state.
    Optimized to return input immediately if no history exists.
    """
    items = await previous_data_storage.get(set_name)
    
    match items:
        case None:
            # If no history exists, all input items are considered new
            return input
        case _:
            history = items.get(CompareTo.ALL.value, [])
            
            match history:
                case []:
                    # If history is empty, all input items are considered new
                    return input
                case _:
                    # Filter out items that are already in history
                    return [item for item in input if item not in history]


@async_ex_to_error_result(StorageError.from_exception)
async def _apply_readonly_filter_last(set_name: str, input: FilterNewDataInput) -> list[DataDto]:
    """
    Filters out items present in the last batch (LAST mode) without updating state.
    Optimized to return input immediately if no history exists.
    """
    items = await previous_data_storage.get(set_name)
    
    match items:
        case None:
            # If no history exists, all input items are considered new
            return input
        case _:
            history = items.get(CompareTo.LAST.value, [])
            
            match history:
                case []:
                    # If history is empty, all input items are considered new
                    return input
                case _:
                    # Filter out items that are already in history
                    return [item for item in input if item not in history]


# Dictionary mapping CompareTo modes to their respective handler functions
_COMPARE_TO_STRATEGIES: dict[CompareTo, CompareToStrategyFunc] = {
    CompareTo.ALL: _apply_append_to_all,
    CompareTo.LAST: _apply_replace_last,
    CompareTo.ALL_READONLY: _apply_readonly_filter_all,
    CompareTo.LAST_READONLY: _apply_readonly_filter_last,
}
