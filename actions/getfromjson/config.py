from dataclasses import dataclass
from enum import StrEnum
import functools
from typing import Any, NamedTuple, Optional

from expression import Result

from shared.utils.parse import NonEmptyStr, parse_bool_str, parse_dict_field
from shared.utils.result import apply, apply4, traverse_accumulating_with_index
from shared.utils.string import strip_and_lowercase
from shared.validation import ValueError as ValueErr, ValueInvalid, with_prefix


class Operation(StrEnum):
    QUERY = "query"
    FILTER = "filter"


class Mode(StrEnum):
    SINGLE = "single"
    ALL = "all"

    @staticmethod
    def parse(value: str) -> Optional["Mode"]:
        if value is None:
            return None
        match strip_and_lowercase(value):
            case Mode.SINGLE.value:
                return Mode.SINGLE
            case Mode.ALL.value:
                return Mode.ALL
            case _:
                return None


class Parser(StrEnum):
    JSONPATH_NG = "jsonpath-ng"
    JMESPATH = "jmespath"

    @staticmethod
    def parse(parser: str) -> Optional["Parser"]:
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
    """Query operation configuration."""

    query: NonEmptyStr
    output_name: NonEmptyStr | None
    default_value: DefaultValueConfig | None
    mode: Mode

    @staticmethod
    def from_dict(data: dict) -> Result["GetFromJsonQuery", tuple[ValueErr, ...]]:
        def validate_query() -> Result[NonEmptyStr, ValueErr]:
            return parse_dict_field(data, "query", NonEmptyStr.parse)

        def validate_output_name() -> Result[NonEmptyStr | None, ValueErr]:
            if "output_name" not in data:
                return Result.Ok(None)
            return parse_dict_field(data, "output_name", NonEmptyStr.parse)

        def get_default_value() -> DefaultValueConfig | None:
            if "default_value" not in data:
                return None
            return DefaultValueConfig(data["default_value"])

        def validate_mode() -> Result[Mode, ValueErr]:
            if "mode" not in data:
                return Result.Ok(Mode.SINGLE)
            return parse_dict_field(data, "mode", Mode.parse)

        query_res = validate_query()
        output_name_res = validate_output_name()
        opt_default_value = get_default_value()
        mode_res = validate_mode()

        config_res = apply4(
            GetFromJsonQuery,
            lambda errs: errs,
            query_res,
            output_name_res,
            Result.Ok(opt_default_value),
            mode_res,
        )
        return config_res


class GetFromJsonFilter(NonEmptyStr):
    """Filter operation configuration. Inherits from NonEmptyStr for type safety and str compatibility."""

    def __repr__(self) -> str:
        return f"GetFromJsonFilter({str.__repr__(self)})"
    
    @staticmethod
    def from_dict(data: dict) -> Result["GetFromJsonFilter", tuple[ValueErr, ...]]:
        filter_str_res = parse_dict_field(data, "filter", GetFromJsonFilter.parse)
        return filter_str_res.map_error(lambda err: tuple([err]))


@dataclass(frozen=True)
class GetFromJsonOperationConfig:
    operation: Operation
    parser: Parser
    data: GetFromJsonQuery | GetFromJsonFilter

    @staticmethod
    def from_dict(
        data: dict,
    ) -> Result["GetFromJsonOperationConfig", tuple[ValueErr, ...]]:
        def parse_operation() -> Result[Operation, ValueErr]:
            is_query_operation = "query" in data
            is_filter_operation = "filter" in data
            match is_query_operation, is_filter_operation:
                case True, False:
                    return Result.Ok(Operation.QUERY)
                case False, True:
                    return Result.Ok(Operation.FILTER)
                case _:
                    return Result.Error(ValueInvalid("*", data))

        def parse_parser() -> Result[Parser, ValueErr]:
            if "parser" not in data:
                return Result.Ok(Parser.JMESPATH)
            return parse_dict_field(data, "parser", Parser.parse)

        def validate_config(
            operation: Operation, parser: Parser
        ) -> Result["GetFromJsonOperationConfig", tuple[ValueErr, ...]]:
            match operation:
                case Operation.QUERY:
                    return GetFromJsonQuery.from_dict(data).map(
                        lambda q: GetFromJsonOperationConfig(operation, parser, q)
                    )
                case Operation.FILTER:
                    return GetFromJsonFilter.from_dict(data).map(
                        lambda f: GetFromJsonOperationConfig(operation, parser, f)
                    )

        operation_res = parse_operation()
        parser_res = parse_parser()

        op_with_parser_res = apply(
            lambda operation, parser: (operation, parser),
            lambda errs: errs,
            operation_res,
            parser_res,
        )

        config_res = op_with_parser_res.bind(
            lambda op_with_parser: validate_config(*op_with_parser)
        )
        return config_res


@dataclass(frozen=True)
class GetFromJsonConfig:
    operations: tuple[GetFromJsonOperationConfig, ...]
    return_empty_result: bool

    @staticmethod
    def from_dict(
        data: dict[str, Any],
    ) -> Result["GetFromJsonConfig", tuple[ValueErr, ...]]:
        def validate_raw_operation(
            idx: int, 
            raw_operation: Any,
        ):
            match raw_operation:
                case dict():
                    return GetFromJsonOperationConfig.from_dict(raw_operation) \
                        .map_error(functools.partial(with_prefix, f"operations[{idx}]"))
                case _:
                    return Result[GetFromJsonOperationConfig, ValueErr].Error(
                        ValueInvalid(f"operations[{idx}]", raw_operation)
                    )

        def validate_operations() -> (
            Result[tuple[GetFromJsonOperationConfig, ...], tuple[ValueErr, ...]]
        ):
            raw_operations_res = parse_dict_field(
                data,
                "operations",
                lambda ops: ops if isinstance(ops, list) and ops else None,
            ).map_error(lambda err: tuple([err]))
            return raw_operations_res.bind(
                lambda raw_ops: traverse_accumulating_with_index(
                    raw_ops, validate_raw_operation
                ).map(tuple)
            )

        def validate_return_empty_result() -> Result[bool, ValueErr]:
            if "return_empty_result" not in data:
                return Result.Ok(False)
            return parse_dict_field(data, "return_empty_result", parse_bool_str)

        operations_res = validate_operations()
        return_empty_result_res = validate_return_empty_result()

        config_res = apply(
            GetFromJsonConfig,
            lambda errs: errs,
            operations_res,
            return_empty_result_res,
        )
        return config_res
