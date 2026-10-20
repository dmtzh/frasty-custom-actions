from dataclasses import dataclass
from itertools import chain
import re
from typing import Any

from expression import Result

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import Error
from shared.pipeline.actionhandler import DataDto
from shared.utils.exceptiondecorators import ex_to_error_result
from shared.utils.parse import DefaultValue, NonEmptyStr, parse_bool_str, parse_dict_field, parse_optional_default_value, parse_val
from shared.utils.result import apply4
from shared.validation import ValueError as ValueErr, format_value_errors


from customactionhandler import CustomActionHandler

@dataclass(frozen=True)
class ApplyRegexConfig:
    field_name: NonEmptyStr
    expression: re.Pattern[str]
    default_value: DefaultValue | None
    return_empty_result: bool

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['ApplyRegexConfig', tuple[ValueErr, ...]]:
        def validate_field_name() -> Result[NonEmptyStr, ValueErr]:
            return parse_dict_field(data, "field_name", NonEmptyStr.parse)

        def validate_expression() -> Result[re.Pattern[str], ValueErr]:
            def compile_regex(expr: NonEmptyStr) -> re.Pattern[str] | None:
                try:
                    return re.compile(expr)
                except re.error:
                    return None

            raw_expression_res = parse_dict_field(data, "expression", NonEmptyStr.parse)
            expression_res = raw_expression_res.bind(lambda raw_expr: parse_val(raw_expr, "expression", compile_regex))
            return expression_res

        def validate_return_empty_result() -> Result[bool, ValueErr]:
            if "return_empty_result" not in data:
                return Result.Ok(False)
            return parse_dict_field(data, "return_empty_result", parse_bool_str)

        field_name_res = validate_field_name()
        expression_res = validate_expression()
        opt_default_value_res: Result[DefaultValue | None, ValueErr] = Result.Ok(parse_optional_default_value(data))
        return_empty_result_res = validate_return_empty_result()

        return apply4(ApplyRegexConfig, lambda err: err, field_name_res, expression_res, opt_default_value_res, return_empty_result_res)

type ApplyRegexInput = list[DataDto]

class ApplyRegexHandler(CustomActionHandler[ApplyRegexConfig, ApplyRegexInput]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("applyregex")
    
    def validate_config(self, raw_config: dict[str, Any]) -> Result[ApplyRegexConfig, Any]:
        return ApplyRegexConfig.from_dict(raw_config).map_error(format_value_errors)
    
    def validate_input(self, _: ApplyRegexConfig, dto_list: list[DataDto]) -> Result[ApplyRegexInput, Any]:
        return Result.Ok(dto_list)
    
    async def handle(self, config: ApplyRegexConfig, input_list: ApplyRegexInput) -> CompletedResult:
        def apply_to_input_field(input: DataDto):
            if config.field_name not in input:
                return [input]
            text = input[config.field_name]
            matches = [match.group(0) for match in re.finditer(config.expression, text)]
            match matches:
                case [] if config.default_value is not None:
                    # No matches + default_value is set -> substitute the default value.
                    return [input | {config.field_name: config.default_value.value}]
                case []:
                    # No matches + no default_value -> empty list (element dropped).
                    return []
                case _:
                    # Matches found -> one DataDto per match (default_value is ignored).
                    return [input | {config.field_name: match} for match in matches]
        @ex_to_error_result(Error.from_exception)
        def apply_to_input_list() -> list[DataDto]:
            flat_mapped_iter = chain.from_iterable(map(apply_to_input_field, input_list))
            return list(flat_mapped_iter)
        def ok_to_completed_result(result_data: list[DataDto]):
            return CompletedWith.Data(result_data) if result_data or config.return_empty_result else CompletedWith.NoData()
        def err_to_completed_result(err):
            return CompletedWith.Error(str(err))
        
        apply_res = apply_to_input_list()
        return apply_res.map(ok_to_completed_result).default_with(err_to_completed_result)