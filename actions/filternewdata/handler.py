from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Optional

from expression import Result

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import Error
from shared.infrastructure.storage.repository import StorageError
from shared.pipeline.actionhandler import DataDto
from shared.utils.asyncresult import async_result, AsyncResult, coroutine_result
from shared.utils.exceptiondecorators import async_ex_to_error_result
from shared.utils.parse import parse_from_dict, NonEmptyStr
from shared.utils.result import apply3, traverse_accumulating_with_index
from shared.utils.string import strip_and_lowercase
from shared.validation import ValueInvalid

from customactionhandler import CustomActionHandler

from .previousdatastore import PreviousDataStore
from .projection import ProjectionError, ProjectionResolver, create_projection_resolver

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
    set_name: NonEmptyStr
    compare_to: CompareTo
    resolver: ProjectionResolver

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['FilterNewDataConfig', str]:
        def validate_set_name() -> Result[NonEmptyStr, str]:
            return parse_from_dict(data, "set_name", NonEmptyStr.parse)

        def validate_compare_to() -> Result[CompareTo, str]:
            if "compare_to" not in data:
                return Result.Ok(CompareTo.ALL)
            return parse_from_dict(data, "compare_to", CompareTo.parse)

        def validate_projection() -> Result[ProjectionResolver, str]:
            if "projection" not in data:
                return create_projection_resolver(None).map_error(lambda err: err.message)
            projection_res = parse_from_dict(data, "projection", NonEmptyStr.parse).map_error(ProjectionError)
            resolver_res = projection_res.bind(create_projection_resolver)
            return resolver_res.map_error(lambda err: err.message)

        set_name_res = validate_set_name()
        compare_to_res = validate_compare_to()
        resolver_res = validate_projection()
        
        config_res = apply3(FilterNewDataConfig, ", ".join, set_name_res, compare_to_res, resolver_res)
        return config_res

type FilterNewDataInput = list[ProjectedDataDto]

@dataclass(frozen=True)
class ProjectedDataDto:
    dto: DataDto
    projection: DataDto

class FilterNewDataHandler(CustomActionHandler[FilterNewDataConfig, FilterNewDataInput]):
    """
    Handler that filters out items already present in the previous data storage.
    
    Key behaviors:
      - Supports four comparison modes:
          * ALL: Accumulates all unique items seen so far
          * LAST: Compares with the last batch only
          * ALL_READONLY: Filters against accumulated history without updating
          * LAST_READONLY: Filters against last batch without updating
      - Uses PreviousDataStore for persistent storage across pipeline runs
      - Strategies are created once in __init__ as closures capturing the storage
    """

    def __init__(self, previous_data_storage: PreviousDataStore) -> None:
        self._get_strategy = _create_strategies(previous_data_storage)

    @property
    def action_name(self) -> ActionName:
        return ActionName("filternewdata")
    
    def validate_config(self, raw_config: dict[str, Any]) -> Result[FilterNewDataConfig, Any]:
        return FilterNewDataConfig.from_dict(raw_config)
    
    def validate_input(self, config: FilterNewDataConfig, dto_list: list[DataDto]) -> Result[FilterNewDataInput, Any]:
        # convert input to projected input
        def dto_to_projected_dto(idx: int, dto: DataDto):
            return config.resolver.apply(dto)\
                .map(lambda projection: ProjectedDataDto(dto, projection))\
                .map_error(lambda err: ProjectionError(f"input_data[{idx}]: {err.message}"))

        projected_input_res = traverse_accumulating_with_index(dto_list, dto_to_projected_dto)
        return projected_input_res
    
    async def handle(self, config: FilterNewDataConfig, input: FilterNewDataInput) -> CompletedResult:
        @coroutine_result[Error | StorageError]()
        async def filter_new_data_workflow():
            # Get the strategy function from the dictionary
            strategy_func_res = self._get_strategy(config.compare_to)\
                .map_error(lambda err: Error(f"Invalid configuration: unknown compare_to mode '{err.name}'"))
            strategy_func = await AsyncResult.from_result(strategy_func_res)

            # Execute the strategy
            new_data = await async_result(strategy_func)(config.set_name, input)
            return new_data
            
        def ok_to_completed_result(result_data: list[DataDto]):
            return CompletedWith.Data(result_data) if result_data else CompletedWith.NoData()
        def err_to_completed_result(err):
            return CompletedWith.Error(str(err))

        new_data_res = await filter_new_data_workflow()
        completed_result = new_data_res.map(ok_to_completed_result).default_with(err_to_completed_result)
        return completed_result

# --- Module-level compare to strategy functions ---
type CompareToStrategyFunc = Callable[[str, FilterNewDataInput], Coroutine[Any, Any, Result[list[DataDto], StorageError]]]

def _create_strategies(previous_data_storage: PreviousDataStore):
    """
    Create strategy functions as closures capturing the storage instance.
    This allows using the @with_storage decorator while maintaining DI.
    """
    storage = previous_data_storage

    @async_ex_to_error_result(StorageError.from_exception)
    @storage.with_storage
    def apply_append_to_all(items: dict[str, list[DataDto]] | None, input: FilterNewDataInput) -> tuple[list[DataDto], dict[str, list[DataDto]]]:
        """
        Accumulates all unique items seen so far for the given set_name.
        Returns new unique items and the updated state.
        """
        match items:
            case None:
                return ([item.dto for item in input], {CompareTo.ALL.value: [item.projection for item in input]})
            case _:
                if CompareTo.ALL.value not in items:
                    return ([item.dto for item in input], items | {CompareTo.ALL.value: [item.projection for item in input]})
                history = items[CompareTo.ALL.value]
                new_items = [item for item in input if item.projection not in history]
                updated_history = history + [item.projection for item in new_items]
                return ([item.dto for item in new_items], items | {CompareTo.ALL.value: updated_history})

    @async_ex_to_error_result(StorageError.from_exception)
    @storage.with_storage
    def apply_replace_last(items: dict[str, list[DataDto]] | None, input: FilterNewDataInput) -> tuple[list[DataDto], dict[str, list[DataDto]]]:
        """
        Compares input with the last batch of items seen for the given set_name.
        Updates the state to reflect the current input as the last batch.
        """
        match items:
            case None:
                return ([item.dto for item in input], {CompareTo.LAST.value: [item.projection for item in input]})
            case _:
                if CompareTo.LAST.value not in items:
                    return ([item.dto for item in input], items | {CompareTo.LAST.value: [item.projection for item in input]})
                last_batch = items[CompareTo.LAST.value]
                new_items = [item.dto for item in input if item.projection not in last_batch]
                updated_history = [item.projection for item in input]
                return (new_items, items | {CompareTo.LAST.value: updated_history})

    @async_ex_to_error_result(StorageError.from_exception)
    async def apply_readonly_filter_all(set_name: str, input: FilterNewDataInput) -> list[DataDto]:
        """
        Filters out items already present in the accumulated history (ALL mode) without updating state.
        """
        items = await storage.get(set_name)
        match items:
            case None:
                return [item.dto for item in input]
            case _:
                history = items.get(CompareTo.ALL.value, [])
                match history:
                    case []:
                        return [item.dto for item in input]
                    case _:
                        return [item.dto for item in input if item.projection not in history]

    @async_ex_to_error_result(StorageError.from_exception)
    async def apply_readonly_filter_last(set_name: str, input: FilterNewDataInput) -> list[DataDto]:
        """
        Filters out items present in the last batch (LAST mode) without updating state.
        """
        items = await storage.get(set_name)
        match items:
            case None:
                return [item.dto for item in input]
            case _:
                history = items.get(CompareTo.LAST.value, [])
                match history:
                    case []:
                        return [item.dto for item in input]
                    case _:
                        return [item.dto for item in input if item.projection not in history]

    strategies = {
        CompareTo.ALL: apply_append_to_all,
        CompareTo.LAST: apply_replace_last,
        CompareTo.ALL_READONLY: apply_readonly_filter_all,
        CompareTo.LAST_READONLY: apply_readonly_filter_last
    }
    def get_strategy(compare_to: CompareTo) -> Result[CompareToStrategyFunc, ValueInvalid]:
        match strategies.get(compare_to):
            case None:
                return Result.Error(ValueInvalid(compare_to))
            case strategy:
                return Result.Ok(strategy)
    return get_strategy