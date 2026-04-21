from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Optional

from expression import Result

from shared.utils.parse import parse_bool_str, parse_from_dict, parse_non_empty_str
from shared.utils.result import to_error_list
from shared.utils.string import strip_and_lowercase

class Operation(StrEnum):
    QUERY = "query"
    JMESPATHQUERY = "jmespathquery"
    FILTER = "filter"
    MAP = "map"

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

@dataclass(frozen=True)
class GetFromJsonQuery:
    '''Query operation configuration'''
    query: str
    parser: Parser
    output_name: str | None
    default_value: Any | None

    @staticmethod
    def from_dict(data: dict) -> Result['GetFromJsonQuery', str]:
        def validate_query() -> Result[str, str]:
            return parse_from_dict(data, "query", parse_non_empty_str)
        def validate_parser() -> Result[Parser, str]:
            if "parser" in data:
                return parse_from_dict(data, "parser", Parser.parse)
            else:
                return Result.Ok(Parser.JSONPATH_NG)
        def validate_output_name() -> Result[str | None, str]:
            if "output_name" not in data:
                return Result.Ok(None)
            return parse_from_dict(data, "output_name", parse_non_empty_str)
        query_res = validate_query()
        parser_res = validate_parser()
        output_name_res = validate_output_name()
        default_value = data.get("default_value")
        errs = to_error_list(query_res, parser_res, output_name_res)
        match errs:
            case []:
                return Result.Ok(GetFromJsonQuery(query_res.ok, parser_res.ok, output_name_res.ok, default_value))
            case _:
                return Result.Error(", ".join(errs))

class GetFromJsonFilter(str):
    '''Filter operation configuration'''

    @staticmethod
    def from_dict(data: dict) -> Result['GetFromJsonFilter', str]:
        filter_str_res = parse_from_dict(data, "filter", parse_non_empty_str)
        return filter_str_res.map(GetFromJsonFilter)

class GetFromJsonMap(str):
    '''Map operation configuration'''

    @staticmethod
    def from_dict(data: dict) -> Result['GetFromJsonMap', str]:
        filter_str_res = parse_from_dict(data, "map", parse_non_empty_str)
        return filter_str_res.map(GetFromJsonMap)

@dataclass(frozen=True)
class GetFromJsonOperationConfig:
    operation: Operation
    data: GetFromJsonQuery | GetFromJsonFilter | GetFromJsonMap

    @staticmethod
    def _from_query(query: GetFromJsonQuery):
        match query.parser:
            case Parser.JSONPATH_NG:
                return GetFromJsonOperationConfig(Operation.QUERY, query)
            case Parser.JMESPATH:
                return GetFromJsonOperationConfig(Operation.JMESPATHQUERY, query)
        return GetFromJsonOperationConfig(Operation.QUERY, query)

    @staticmethod
    def _from_filter(filter: GetFromJsonFilter):
        return GetFromJsonOperationConfig(Operation.FILTER, filter)
    
    @staticmethod
    def _from_map(map: GetFromJsonMap):
        return GetFromJsonOperationConfig(Operation.MAP, map)

    @staticmethod
    def from_dict(data: dict) -> Result['GetFromJsonOperationConfig', str]:
        def parse_operation() -> Result[Operation, str]:
            is_query_operation = "query" in data
            is_filter_operation = "filter" in data
            is_map_operation = "map" in data
            match is_query_operation, is_filter_operation, is_map_operation:
                case True, False, False:
                    return Result.Ok(Operation.QUERY)
                case False, True, False:
                    return Result.Ok(Operation.FILTER)
                case False, False, True:
                    return Result.Ok(Operation.MAP)
                case _:
                    return Result.Error(f"invalid 'operation' value {data}")
        def validate_query_config() -> Result[GetFromJsonOperationConfig, str]:
            query_res = GetFromJsonQuery.from_dict(data)
            return query_res.map(GetFromJsonOperationConfig._from_query)
        def validate_filter_config() -> Result[GetFromJsonOperationConfig, str]:
            filter_res = GetFromJsonFilter.from_dict(data)
            return filter_res.map(GetFromJsonOperationConfig._from_filter)
        def validate_map_config() -> Result[GetFromJsonOperationConfig, str]:
            map_res = GetFromJsonMap.from_dict(data)
            return map_res.map(GetFromJsonOperationConfig._from_map)
        def validate_config(operation: Operation) -> Result[GetFromJsonOperationConfig, str]:
            match operation:
                case Operation.QUERY | Operation.JMESPATHQUERY:
                    return validate_query_config()
                case Operation.FILTER:
                    return validate_filter_config()
                case Operation.MAP:
                    return validate_map_config()
        operation_res = parse_operation()
        config_res = operation_res.bind(validate_config)
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
            operations_res_list = [validate_raw_operation(raw_operation) for raw_operation in raw_operations]
            errs = to_error_list(*operations_res_list)
            match errs:
                case []:
                    operations_tuple = tuple(operation_res.ok for operation_res in operations_res_list)
                    return Result.Ok(operations_tuple)
                case _:
                    return Result.Error(", ".join(errs))
        def validate_operations() -> Result[tuple[GetFromJsonOperationConfig, ...], str]:
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