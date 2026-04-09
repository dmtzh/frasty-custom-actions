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
class GetContentFromHtmlConfig:
    css_selector: str | None
    regex_selector: str | None
    output_name: str | None
    return_empty_result: bool

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['GetContentFromHtmlConfig', str]:
        def validate_has_any_selector() -> Result[None, str]:
            has_any_selector = "css_selector" in data or "regex_selector" in data
            return Result.Ok(None) if has_any_selector else Result.Error("css_selector is missing")
        def validate_css_selector() -> Result[str | None, str]:
            if "css_selector" not in data:
                return Result.Ok(None)
            return parse_from_dict(data, "css_selector", parse_non_empty_str)
        def validate_regex_selector() -> Result[str | None, str]:
            if "regex_selector" not in data:
                return Result.Ok(None)
            return parse_from_dict(data, "regex_selector", parse_non_empty_str)
        def validate_output_name() -> Result[str | None, str]:
            if "output_name" not in data:
                return Result.Ok(None)
            return parse_from_dict(data, "output_name", parse_non_empty_str)
        def validate_return_empty_result() -> Result[bool, str]:
            if "return_empty_result" not in data:
                return Result.Ok(False)
            return parse_from_dict(data, "return_empty_result", parse_bool_str)
        has_any_selector_res = validate_has_any_selector()
        css_selector_res = validate_css_selector()
        regex_selector_res = validate_regex_selector()
        output_name_res = validate_output_name()
        return_empty_result_res = validate_return_empty_result()
        errs = to_error_list(has_any_selector_res, css_selector_res, regex_selector_res, output_name_res, return_empty_result_res)
        match errs:
            case []:
                return Result.Ok(GetContentFromHtmlConfig(css_selector_res.ok, regex_selector_res.ok, output_name_res.ok, return_empty_result_res.ok))
            case _:
                return Result.Error(", ".join(errs))

type GetContentFromHtmlInput = list[DataDto]

class GetContentFromHtmlHandler(CustomActionHandler[GetContentFromHtmlConfig, GetContentFromHtmlInput]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("getcontentfromhtml")
    
    def validate_config(self, raw_config: dict[str, Any]) -> Result[GetContentFromHtmlConfig, Any]:
        return GetContentFromHtmlConfig.from_dict(raw_config)
    
    def validate_input(self, dto_list: list[DataDto]) -> Result[GetContentFromHtmlInput, Any]:
        return Result.Ok(dto_list)
    
    async def handle(self, config: GetContentFromHtmlConfig, input_list: GetContentFromHtmlInput) -> CompletedResult:
        def get_content_from_input(input: DataDto) -> list[DataDto]:
            if "content" not in input:
                return [input]
            selector = Selector(text=input["content"])
            match config.css_selector, config.regex_selector:
                case None, None:
                    raise ValueError("Selector not specified")
                case None, regex_selector:
                    matches = selector.re(regex_selector)
                case css_selector, _:
                    matches = selector.css(css_selector).getall()
            output_name = config.output_name or "content"
            output_list = [input | {output_name: match} for match in matches]
            return output_list
        @ex_to_error_result(Error.from_exception)
        def get_content_from_input_list() -> list[DataDto]:
            return functools.reduce(lambda acc, curr: acc + get_content_from_input(curr), input_list, [])
        def ok_to_completed_result(result_data: list):
            return CompletedWith.Data(result_data) if result_data or config.return_empty_result else CompletedWith.NoData()
        def err_to_completed_result(err):
            return CompletedWith.Error(str(err))
        
        get_content_res = get_content_from_input_list()
        return get_content_res.map(ok_to_completed_result).default_with(err_to_completed_result)
        