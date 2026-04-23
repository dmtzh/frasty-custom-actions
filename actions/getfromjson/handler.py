from collections.abc import Callable
import functools
from typing import Any

from expression import Result
import jmespath
import jsonpath_ng.ext as jpx

from shared.action import ActionName
from shared.completedresult import CompletedWith, CompletedResult
from shared.customtypes import Error
from shared.pipeline.actionhandler import DataDto
from shared.utils.asyncresult import ex_to_error_result

from customactionhandler import CustomActionHandler

from .config import GetFromJsonFilter, GetFromJsonQuery, Operation, GetFromJsonConfig, GetFromJsonOperationConfig, Parser

type GetFromJsonInput = list[DataDto]
type OperationHandlerFunc = Callable[[list, GetFromJsonOperationConfig], Result[list, Error]]

@ex_to_error_result(Error.from_exception)
def jsonpath_ng_query_handler(input_list, operation: GetFromJsonOperationConfig):
    if not isinstance(operation.data, GetFromJsonQuery):
        raise ValueError(f"Invalid 'operation' value {operation}")
    data = operation.data
    def match_value_to_result(match_value, default_value):
        match match_value:
            case None if default_value is not None:
                return default_value
            case _:
                return match_value
    def query_get_all(input, data: GetFromJsonQuery):
        jp_query = jpx.parse(data.query)
        matches = [match_value_to_result(match.value, data.default_value) for match in jp_query.find(input)]
        return matches
    def get_from_input(input, data: GetFromJsonQuery):
        matches = query_get_all(input, data)
        match data.output_name:
            case None:
                return matches
            case output_name:
                dict_without_output_name = {k:v for k, v in input.items() if k != output_name}
                output_list = [dict_without_output_name | {output_name: match} for match in matches]
                return output_list
    return functools.reduce(lambda acc, curr: acc + get_from_input(curr, data), input_list, [])

@ex_to_error_result(Error.from_exception)
def jmespath_query_handler(input_list, operation: GetFromJsonOperationConfig) -> list:
    if not isinstance(operation.data, GetFromJsonQuery):
        raise ValueError(f"Invalid 'operation' value {operation}")
    expression = f"[].{operation.data.query}"
    return jmespath.search(expression, input_list)

@ex_to_error_result(Error.from_exception)
def jmespath_filter_handler(input_list, operation: GetFromJsonOperationConfig) -> list:
    if not isinstance(operation.data, GetFromJsonFilter):
        raise ValueError(f"Invalid 'operation' value {operation}")
    expression = f"[?{operation.data}]"
    return jmespath.search(expression, input_list)

OPERATION_HANDLERS: dict[tuple[Operation, Parser], OperationHandlerFunc] = {
    (Operation.QUERY, Parser.JMESPATH): jmespath_query_handler,
    (Operation.QUERY, Parser.JSONPATH_NG): jsonpath_ng_query_handler,
    (Operation.FILTER, Parser.JMESPATH): jmespath_filter_handler
}

def dispatch_to_operation_handler(input: list, operation: GetFromJsonOperationConfig) -> Result[list, Error]:
    try:
        return OPERATION_HANDLERS[(operation.operation, operation.parser)](input, operation)
    except KeyError:
        return Result.Error(Error(f"Invalid 'operation' value {operation}"))

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