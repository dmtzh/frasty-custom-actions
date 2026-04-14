from dataclasses import dataclass
from enum import StrEnum
import functools
from typing import Any, Callable

from expression import Result
import jsonpath_ng.ext as jpx

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import Error
from shared.pipeline.actionhandler import DataDto
from shared.utils.asyncresult import ex_to_error_result
from shared.utils.parse import parse_bool_str, parse_from_dict, parse_non_empty_str
from shared.utils.result import to_ok_list, to_error_list

from customactionhandler import CustomActionHandler

class Operation(StrEnum):
    QUERY = "query"

@dataclass(frozen=True)
class GetFromJsonConfigOperation:
    operation: Operation
    query: str
    output_name: str | None
    default_value: Any | None

    @staticmethod
    def from_dict(data: dict) -> Result['GetFromJsonConfigOperation', str]:
        def parse_operation() -> Result[Operation, str]:
            is_query_operation = "query" in data
            match is_query_operation:
                case True:
                    return Result.Ok(Operation.QUERY)
                case False:
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
                    return Result.Ok(GetFromJsonConfigOperation(Operation.QUERY, query_res.ok, output_name_res.ok, default_value))
                case _:
                    return Result.Error(", ".join(errs))
        def validate_config(operation: Operation) -> Result[GetFromJsonConfigOperation, str]:
            match operation:
                case Operation.QUERY:
                    return validate_query_config()
        operation_res = parse_operation()
        config_res = operation_res.bind(validate_config)
        return config_res

@ex_to_error_result(Error.from_exception)
def jsonpath_ng_query_handler(input_list, operation: GetFromJsonConfigOperation):
    def match_value_to_result(match_value, default_value):
        match match_value:
            case None if default_value is not None:
                return default_value
            case _:
                return match_value
    def query_get_all(input, operation: GetFromJsonConfigOperation):
        jp_query = jpx.parse(operation.query)
        matches = [match_value_to_result(match.value, operation.default_value) for match in jp_query.find(input)]
        return matches
    def get_from_input(input, operation: GetFromJsonConfigOperation):
        matches = query_get_all(input, operation)
        match operation.output_name:
            case None:
                return matches
            case output_name:
                dict_without_output_name = {k:v for k, v in input.items() if k != output_name}
                output_list = [dict_without_output_name | {output_name: match} for match in matches]
                return output_list
    return functools.reduce(lambda acc, curr: acc + get_from_input(curr, operation), input_list, [])
type OperationHandlerFunc = Callable[[list, GetFromJsonConfigOperation], Result[list, Error]]
OPERATION_HANDLERS: dict[Operation, OperationHandlerFunc] = {
    Operation.QUERY: jsonpath_ng_query_handler
}
def dispatch_to_operation_handler(input: list, selector: GetFromJsonConfigOperation) -> Result[list, Error]:
    try:
        return OPERATION_HANDLERS[selector.operation](input, selector)
    except KeyError:
        raise ValueError(f"Unsupported operation: {selector.operation}")

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

type GetFromJsonInput = list[DataDto]

class GetFromJsonHandler(CustomActionHandler[GetFromJsonConfig, GetFromJsonInput]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("getfromjson")
    
    def validate_config(self, raw_config: dict[str, Any]) -> Result[GetFromJsonConfig, Any]:
        return GetFromJsonConfig.from_dict(raw_config)
    
    def validate_input(self, dto_list: list[DataDto]) -> Result[GetFromJsonInput, Any]:
        return Result.Ok(dto_list)
    
    async def handle(self, config: GetFromJsonConfig, input_list: GetFromJsonInput) -> CompletedResult:
        def ok_to_completed_result(result_data: list):
            return CompletedWith.Data(result_data) if result_data or config.return_empty_result else CompletedWith.NoData()
        def err_to_completed_result(err):
            return CompletedWith.Error(str(err))
        
        res = functools.reduce(lambda acc_res, operation: acc_res.bind(lambda acc: dispatch_to_operation_handler(acc, operation)), config.operations, Result.Ok(input_list))
        return res.map(ok_to_completed_result).default_with(err_to_completed_result)