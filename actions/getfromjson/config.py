from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Optional

from expression import Result

from shared.utils.parse import parse_bool_str, parse_from_dict, parse_non_empty_str
from shared.utils.result import to_error_list, to_ok_list

class Operation(StrEnum):
    QUERY = "query"
    FILTER = "filter"

class GetFromJsonFilter(str):
    '''Filter operation configuration'''

    @staticmethod
    def parse(raw_filter: str) -> Optional['GetFromJsonFilter']:
        opt_filter = parse_non_empty_str(raw_filter)
        return GetFromJsonFilter(opt_filter) if opt_filter is not None else None

@dataclass(frozen=True)
class GetFromJsonConfigOperation:
    operation: Operation
    query: str | None
    output_name: str | None
    default_value: Any | None
    data: GetFromJsonFilter | None

    @staticmethod
    def _from_filter(filter: GetFromJsonFilter):
        return GetFromJsonConfigOperation(Operation.FILTER, None, None, None, filter)

    @staticmethod
    def from_dict(data: dict) -> Result['GetFromJsonConfigOperation', str]:
        def parse_operation() -> Result[Operation, str]:
            is_query_operation = "query" in data
            is_filter_operation = "filter" in data
            match is_query_operation, is_filter_operation:
                case True, False:
                    return Result.Ok(Operation.QUERY)
                case False, True:
                    return Result.Ok(Operation.FILTER)
                case _:
                    return Result.Error(f"invalid 'operation' value {data}")
        def validate_query_config() -> Result[GetFromJsonConfigOperation, str]:
            def validate_query() -> Result[str, str]:
                return parse_from_dict(data, "query", parse_non_empty_str)
            def validate_output_name() -> Result[str | None, str]:
                if "output_name" not in data:
                    return Result.Ok(None)
                return parse_from_dict(data, "output_name", parse_non_empty_str)
            query_res = validate_query()
            output_name_res = validate_output_name()
            default_value = data.get("default_value")
            errs = to_error_list(query_res, output_name_res)
            match errs:
                case []:
                    return Result.Ok(GetFromJsonConfigOperation(Operation.QUERY, query_res.ok, output_name_res.ok, default_value, None))
                case _:
                    return Result.Error(", ".join(errs))
        def validate_filter_config() -> Result[GetFromJsonConfigOperation, str]:
            def validate_filter() -> Result[GetFromJsonFilter, str]:
                return parse_from_dict(data, "filter", GetFromJsonFilter.parse)
            filter_res = validate_filter()
            return filter_res.map(GetFromJsonConfigOperation._from_filter)
        def validate_config(operation: Operation) -> Result[GetFromJsonConfigOperation, str]:
            match operation:
                case Operation.QUERY:
                    return validate_query_config()
                case Operation.FILTER:
                    return validate_filter_config()
        operation_res = parse_operation()
        config_res = operation_res.bind(validate_config)
        return config_res


@dataclass(frozen=True)
class GetFromJsonConfig:
    operations: tuple[GetFromJsonConfigOperation, ...]
    return_empty_result: bool

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['GetFromJsonConfig', str]:
        def validate_raw_operation(raw_operation) -> Result[GetFromJsonConfigOperation, str]:
            match raw_operation:
                case dict():
                    return GetFromJsonConfigOperation.from_dict(raw_operation)
                case _:
                    return Result.Error(f"invalid 'operation' value {raw_operation}")
        def validate_raw_operations(raw_operations: list[Any]) -> Result[tuple[GetFromJsonConfigOperation, ...], str]:
            operations_res_list = [validate_raw_operation(raw_operation) for raw_operation in raw_operations]
            operations_list = to_ok_list(*operations_res_list)
            match operations_list:
                case []:
                    errs = to_error_list(*operations_res_list)
                    return Result.Error(", ".join(errs))
                case _:
                    return Result.Ok(tuple(operations_list))
        def validate_operations() -> Result[tuple[GetFromJsonConfigOperation, ...], str]:
            raw_operations_res = parse_from_dict(data, "operations", lambda operations: operations if isinstance(operations, list) and operations else None)
            operations_res = raw_operations_res.bind(validate_raw_operations)
            errs = to_error_list(operations_res)
            match errs:
                case []:
                    return Result.Ok(operations_res.ok)
                case _:
                    return Result.Error(", ".join(errs))
        def validate_return_empty_result() -> Result[bool, str]:
            if "return_empty_result" not in data:
                return Result.Ok(False)
            return parse_from_dict(data, "return_empty_result", parse_bool_str)
        operations_res = validate_operations()
        return_empty_result_res = validate_return_empty_result()
        errs = to_error_list(operations_res, return_empty_result_res)
        match errs:
            case []:
                return Result.Ok(GetFromJsonConfig(operations_res.ok, return_empty_result_res.ok))
            case _:
                return Result.Error(", ".join(errs))