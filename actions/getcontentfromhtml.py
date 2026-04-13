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
from shared.utils.result import to_error_list, to_ok_list

from customactionhandler import CustomActionHandler

class GetContentFromHtmlConfigSelector:
    class CssSelector:
        def __init__(self, css: str):
            self._css = css
        def process_content(self, content) -> list[str]:
            return Selector(text=content).css(self._css).getall()
    
    class RegexSelector:
        def __init__(self, regex: str):
            self._regex = regex
        def process_content(self, content) -> list[str]:
            return Selector(text=content).re(self._regex)

    def __init__(self, selector: CssSelector | RegexSelector, output_name: str | None):
        self._selector = selector
        self._output_name = output_name

    def process_input(self, input: DataDto) -> list[DataDto]:
        content = input["content"]
        matches = self._selector.process_content(content)
        output_name = self._output_name or "content"
        output_list = [input | {output_name: match} for match in matches]
        return output_list
    
    @staticmethod
    def from_dict(data: dict) -> Result['GetContentFromHtmlConfigSelector', str]:
        def validate_css_selector() -> Result[GetContentFromHtmlConfigSelector.CssSelector, str] | None:
            if "css" not in data:
                return None
            return parse_from_dict(data, "css", parse_non_empty_str).map(GetContentFromHtmlConfigSelector.CssSelector)
        def validate_regex_selector() -> Result[GetContentFromHtmlConfigSelector.RegexSelector, str] | None:
            if "regex" not in data:
                return None
            return parse_from_dict(data, "regex", parse_non_empty_str).map(GetContentFromHtmlConfigSelector.RegexSelector)
        def validate_output_name() -> Result[str | None, str]:
            if "output_name" not in data:
                return Result.Ok(None)
            return parse_from_dict(data, "output_name", parse_non_empty_str)
        
        opt_selector_res = validate_css_selector() or validate_regex_selector()
        if opt_selector_res is None:
            return Result.Error("css or regex selector is missing")
        output_name_res = validate_output_name()
        errs = to_error_list(opt_selector_res, output_name_res)
        match errs:
            case []:
                return Result.Ok(GetContentFromHtmlConfigSelector(opt_selector_res.ok, output_name_res.ok))
            case _:
                return Result.Error(", ".join(errs))

@dataclass(frozen=True)
class GetContentFromHtmlConfig:
    selectors: tuple[GetContentFromHtmlConfigSelector, ...]
    return_empty_result: bool

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['GetContentFromHtmlConfig', str]:
        def validate_raw_selector(raw_selector) -> Result[GetContentFromHtmlConfigSelector, str]:
            match raw_selector:
                case dict():
                    return GetContentFromHtmlConfigSelector.from_dict(raw_selector)
                case _:
                    return Result.Error(f"invalid 'selector' value {raw_selector}")    
        def validate_raw_selectors(raw_selectors: list[Any]) -> Result[tuple[GetContentFromHtmlConfigSelector, ...], str]:
            selectors_res_list = [validate_raw_selector(raw_selector) for raw_selector in raw_selectors]
            selectors_list = to_ok_list(*selectors_res_list)
            match selectors_list:
                case []:
                    errs = to_error_list(*selectors_res_list)
                    return Result.Error(", ".join(errs))
                case _:
                    return Result.Ok(tuple(selectors_list))
        def validate_selectors() -> Result[tuple[GetContentFromHtmlConfigSelector, ...], str]:
            raw_selectors_res = parse_from_dict(data, "selectors", lambda selectors: selectors if isinstance(selectors, list) and selectors else None)
            return raw_selectors_res.bind(validate_raw_selectors)
        def validate_return_empty_result() -> Result[bool, str]:
            if "return_empty_result" not in data:
                return Result.Ok(False)
            return parse_from_dict(data, "return_empty_result", parse_bool_str)
        
        selectors_res = validate_selectors()
        return_empty_result_res = validate_return_empty_result()
        errs = to_error_list(selectors_res, return_empty_result_res)
        match errs:
            case []:
                return Result.Ok(GetContentFromHtmlConfig(selectors_res.ok, return_empty_result_res.ok))
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
        def get_content_from_input(input: DataDto, selector: GetContentFromHtmlConfigSelector) -> list[DataDto]:
            if "content" not in input:
                return [input]
            output_list = selector.process_input(input)
            return output_list
        @ex_to_error_result(Error.from_exception)
        def get_content_from_input_list(input_list: list[DataDto], selector: GetContentFromHtmlConfigSelector) -> list[DataDto]:
            return functools.reduce(lambda acc, curr: acc + get_content_from_input(curr, selector), input_list, [])
        def ok_to_completed_result(result_data: list):
            return CompletedWith.Data(result_data) if result_data or config.return_empty_result else CompletedWith.NoData()
        def err_to_completed_result(err):
            return CompletedWith.Error(str(err))
        
        res = functools.reduce(lambda acc_res, selector: acc_res.bind(lambda acc: get_content_from_input_list(acc, selector)), config.selectors, Result.Ok(input_list))
        return res.map(ok_to_completed_result).default_with(err_to_completed_result)
        