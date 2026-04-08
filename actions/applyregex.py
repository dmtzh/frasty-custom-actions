from dataclasses import dataclass
import functools
from typing import Any

from expression import Result
from parsel import Selector

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import Error
from shared.pipeline.actionhandler import DataDto
from shared.utils.asyncresult import ex_to_error_result
from shared.utils.parse import parse_bool_str, parse_from_dict, parse_non_empty_str
from shared.utils.result import to_error_list

from customactionhandler import CustomActionHandler

@dataclass(frozen=True)
class ApplyRegexConfig:
    field_name: str
    expression: str
    return_empty_result: bool

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['ApplyRegexConfig', str]:
        def validate_field_name() -> Result[str, str]:
            return parse_from_dict(data, "field_name", parse_non_empty_str)
        def validate_expression() -> Result[str, str]:
            return parse_from_dict(data, "expression", parse_non_empty_str)
        def validate_return_empty_result() -> Result[bool, str]:
            if "return_empty_result" not in data:
                return Result.Ok(False)
            return parse_from_dict(data, "return_empty_result", parse_bool_str)
        field_name_res = validate_field_name()
        expression_res = validate_expression()
        return_empty_result_res = validate_return_empty_result()
        errs = to_error_list(field_name_res, expression_res, return_empty_result_res)
        match errs:
            case []:
                return Result.Ok(ApplyRegexConfig(field_name_res.ok, expression_res.ok, return_empty_result_res.ok))
            case _:
                return Result.Error(", ".join(errs))

type ApplyRegexInput = list[DataDto]

class ApplyRegexHandler(CustomActionHandler[ApplyRegexConfig, ApplyRegexInput]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("applyregex")
    
    def validate_config(self, raw_config: dict[str, Any]) -> Result[ApplyRegexConfig, Any]:
        return ApplyRegexConfig.from_dict(raw_config)
    
    def validate_input(self, dto_list: list[DataDto]) -> Result[ApplyRegexInput, Any]:
        return Result.Ok(dto_list)
    
    async def handle(self, config: ApplyRegexConfig, input_list: ApplyRegexInput) -> CompletedResult:
        def apply_to_input_field(input: DataDto):
            if config.field_name not in input:
                return [input]
            selector = Selector(text=input[config.field_name])
            matches = selector.re(config.expression)
            output_list = [input | {config.field_name: match} for match in matches]
            return output_list
        @ex_to_error_result(Error.from_exception)
        def apply_to_input_list() -> list[DataDto]:
            return functools.reduce(lambda acc, curr: acc + apply_to_input_field(curr), input_list, [])
        def ok_to_completed_result(result_data: list[DataDto]):
            return CompletedWith.Data(result_data) if result_data or config.return_empty_result else CompletedWith.NoData()
        def err_to_completed_result(err):
            return CompletedWith.Error(str(err))
        
        apply_res = apply_to_input_list()
        return apply_res.map(ok_to_completed_result).default_with(err_to_completed_result)