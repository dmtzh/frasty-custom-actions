from dataclasses import dataclass
from enum import StrEnum
import functools
from typing import Any, NamedTuple, Optional

from expression import Result

from shared.utils.parse import parse_bool_str, parse_from_dict, parse_non_empty_str
from shared.utils.result import apply
from shared.utils.string import strip_and_lowercase

class Operation(StrEnum):
    QUERY = "query"
    FILTER = "filter"

class Parser(StrEnum):
    JSONPATH_NG = "jsonpath-ng"
    JMESPATH = "jmespath"

    @staticmethod
    def parse(parser: str) -> Optional['Parser']:
        if parser is None:
            return None
        match strip_and_lowercase(parser):
            case Parser.JSONPATH_NG.value:
                return Parser.JSONPATH_NG
            case Parser.JMESPATH.value:
                return Parser.JMESPATH
            case _:
                return None

class DefaultValueConfig(NamedTuple):
    value: Any

@dataclass(frozen=True)
class GetFromJsonQuery:
    '''Query operation configuration'''
    query: str
    output_name: str | None
    default_value: DefaultValueConfig | None

    @staticmethod
    def from_dict(data: dict) -> Result['GetFromJsonQuery', str]:
        def validate_query() -> Result[str, str]:
            return parse_from_dict(data, "query", parse_non_empty_str)
        def validate_output_name() -> Result[str | None, str]:
            if "output_name" not in data:
                return Result.Ok(None)
            return parse_from_dict(data, "output_name", parse_non_empty_str)
        def get_default_value():
            if "default_value" not in data:
                return None
            return DefaultValueConfig(data["default_value"])
        query_res = validate_query()
        output_name_res = validate_output_name()
        opt_default_value = get_default_value()
        config_res = apply(lambda query, output_name: GetFromJsonQuery(query, output_name, opt_default_value), ", ".join, query_res, output_name_res)
        return config_res

class GetFromJsonFilter(str):
    '''Filter operation configuration'''

    @staticmethod
    def from_dict(data: dict) -> Result['GetFromJsonFilter', str]:
        filter_str_res = parse_from_dict(data, "filter", parse_non_empty_str)
        return filter_str_res.map(GetFromJsonFilter)

@dataclass(frozen=True)
class GetFromJsonOperationConfig:
    operation: Operation
    parser: Parser
    data: GetFromJsonQuery | GetFromJsonFilter

    @staticmethod
    def from_dict(data: dict) -> Result['GetFromJsonOperationConfig', str]:
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
        def parse_parser() -> Result[Parser, str]:
            if "parser" in data:
                return parse_from_dict(data, "parser", Parser.parse)
            else:
                return Result.Ok(Parser.JMESPATH)
        def validate_config(operation: Operation, parser: Parser) -> Result[GetFromJsonOperationConfig, str]:
            match operation:
                case Operation.QUERY:
                    return GetFromJsonQuery.from_dict(data).map(lambda data: GetFromJsonOperationConfig(operation, parser, data))
                case Operation.FILTER:
                    return GetFromJsonFilter.from_dict(data).map(lambda data: GetFromJsonOperationConfig(operation, parser, data))
        operation_res = parse_operation()
        parser_res = parse_parser()
        op_with_parser_res = apply(lambda operation, parser: (operation, parser), ", ".join, operation_res, parser_res)
        config_res = op_with_parser_res.bind(lambda op_with_parser: validate_config(*op_with_parser))
        return config_res

@dataclass(frozen=True)
class GetFromJsonConfig:
    operations: tuple[GetFromJsonOperationConfig, ...]
    return_empty_result: bool

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['GetFromJsonConfig', str]:
        def validate_raw_operation(raw_operation) -> Result[GetFromJsonOperationConfig, str]:
            match raw_operation:
                case dict():
                    return GetFromJsonOperationConfig.from_dict(raw_operation)
                case _:
                    return Result.Error(f"invalid 'operation' value {raw_operation}")
        def validate_raw_operations(raw_operations: list[Any]) -> Result[tuple[GetFromJsonOperationConfig, ...], str]:
            initial_value = Result[tuple[GetFromJsonOperationConfig, ...], tuple[str, ...]].Ok(tuple[GetFromJsonOperationConfig, ...]())
            def reduce_func(ops_res: Result[tuple[GetFromJsonOperationConfig, ...], tuple[str, ...]], raw_op: Any):
                operation_res = validate_raw_operation(raw_op)
                return apply(lambda ops, operation: ops + (operation,), lambda errs: errs, ops_res, operation_res)
            operations_res = functools.reduce(reduce_func, raw_operations, initial_value)
            return operations_res.map_error(", ".join)
        def validate_operations() -> Result[tuple[GetFromJsonOperationConfig, ...], str]:
            raw_operations_res = parse_from_dict(data, "operations", lambda operations: operations if isinstance(operations, list) and operations else None)
            operations_res = raw_operations_res.bind(validate_raw_operations)
            return operations_res
        def validate_return_empty_result() -> Result[bool, str]:
            if "return_empty_result" not in data:
                return Result.Ok(False)
            return parse_from_dict(data, "return_empty_result", parse_bool_str)
        operations_res = validate_operations()
        return_empty_result_res = validate_return_empty_result()
        config_res = apply(GetFromJsonConfig, ", ".join, operations_res, return_empty_result_res)
        return config_res
