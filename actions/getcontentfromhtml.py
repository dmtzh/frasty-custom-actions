from dataclasses import dataclass
import functools
from typing import Any

from expression import Result
from expression.collections.block import Block
from expression.extra.result.traversable import traverse
from parsel import Selector

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import Error
from shared.pipeline.actionhandler import DataDto
from shared.utils.asyncresult import ex_to_error_result
from shared.utils.parse import parse_from_dict, parse_non_empty_str

from customactionhandler import CustomActionHandler

@dataclass(frozen=True)
class GetContentFromHtmlConfig:
    css_selector: str | None
    regex_selector: str | None
    output_name: str | None

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['GetContentFromHtmlConfig', str]:
        def validate_has_any_selector(css_selector, regex_selector) -> Result[None, str]:
            has_any_selector = isinstance(css_selector, str) or isinstance(regex_selector, str)
            return Result.Ok(None) if has_any_selector else Result.Error("css_selector is missing")
        def validate_css_selector(css_selector) -> Result[str | None, str]:
            if not isinstance(css_selector, str):
                return Result.Ok(None)
            return parse_non_empty_str(css_selector, "css_selector")
        def validate_regex_selector(regex_selector) -> Result[str | None, str]:
            if not isinstance(regex_selector, str):
                return Result.Ok(None)
            return parse_non_empty_str(regex_selector, "regex_selector")
        def validate_output_name(output_name) -> Result[str | None, str]:
            if not isinstance(output_name, str):
                return Result.Ok(None)
            return parse_non_empty_str(output_name, "output_name")
        opt_css_selector = data.get("css_selector")
        opt_regex_selector = data.get("regex_selector")
        has_any_selector_res = validate_has_any_selector(opt_css_selector, opt_regex_selector)
        css_selector_res = validate_css_selector(opt_css_selector)
        regex_selector_res = validate_regex_selector(opt_regex_selector)
        opt_output_name = data.get("output_name")
        output_name_res = validate_output_name(opt_output_name)
        errs = [err for err in [has_any_selector_res.swap().default_value(None), css_selector_res.swap().default_value(None), regex_selector_res.swap().default_value(None), output_name_res.swap().default_value(None)] if err is not None]
        match errs:
            case []:
                return Result.Ok(GetContentFromHtmlConfig(css_selector_res.ok, regex_selector_res.ok, output_name_res.ok))
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
        def from_dict(data: DataDto):
            content_res = parse_from_dict(data, "content", lambda content: content if isinstance(content, str) else None)
            return content_res.map(lambda _: data).default_value(None)
        data_list = [data for data in map(from_dict, dto_list) if data is not None]
        match data_list:
            case []:
                return Result.Error("html data is missing")
            case _:
                return Result.Ok(data_list)
    
    async def handle(self, config: GetContentFromHtmlConfig, input: GetContentFromHtmlInput) -> CompletedResult:
        def css_get_all(selector: Selector):
            match config.css_selector:
                case None:
                    return None
                case _:
                    return Result[list[str], Error].Ok(selector.css(config.css_selector).getall())
        def regex_get_all(selector: Selector):
            match config.regex_selector:
                case None:
                    return None
                case _:
                    return Result[list[str], Error].Ok(selector.re(config.regex_selector))
        @ex_to_error_result(Error.from_exception)
        def get_from_content(content: dict):
            selector = Selector(text=content["content"])
            matches_res = css_get_all(selector) or regex_get_all(selector) or Result.Error(Error("Selector not specified"))
            output_name = config.output_name or "content"
            html_without_output_name = {k:v for k, v in content.items() if k != output_name}
            res = matches_res.map(lambda matches: [html_without_output_name | {output_name:match} for match in matches])
            return res
        def ok_to_completed_result(result_data: list):
            return CompletedWith.Data(result_data) if result_data else CompletedWith.NoData()
        def err_to_completed_result(err):
            return CompletedWith.Error(str(err))
        
        contents_res = traverse(get_from_content, Block(input))
        res = contents_res.map(lambda contents: functools.reduce(lambda acc, curr: acc + curr, contents, []))
        return res.map(ok_to_completed_result).default_with(err_to_completed_result)
