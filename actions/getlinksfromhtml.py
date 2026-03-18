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
from shared.utils.parse import parse_bool_str, parse_from_dict, parse_non_empty_str

from customactionhandler import CustomActionHandler

@dataclass(frozen=True)
class GetLinksFromHtmlConfig:
    text_name: str | None
    link_name: str | None
    return_empty_result: bool

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['GetLinksFromHtmlConfig', str]:
        def validate_text_name() -> Result[str | None, str]:
            if "text_name" not in data:
                return Result.Ok(None)
            return parse_from_dict(data, "text_name", parse_non_empty_str)
        def validate_link_name() -> Result[str | None, str]:
            if "link_name" not in data:
                return Result.Ok(None)
            return parse_from_dict(data, "link_name", parse_non_empty_str)
        def validate_return_empty_result() -> Result[bool, str]:
            if "return_empty_result" not in data:
                return Result.Ok(False)
            return parse_from_dict(data, "return_empty_result", parse_bool_str)
        text_name_res = validate_text_name()
        link_name_res = validate_link_name()
        return_empty_result_res = validate_return_empty_result()
        errs = [err for err in [text_name_res.swap().default_value(None), link_name_res.swap().default_value(None), return_empty_result_res.swap().default_value(None)] if err is not None]
        match errs:
            case []:
                return Result.Ok(GetLinksFromHtmlConfig(text_name_res.ok, link_name_res.ok, return_empty_result_res.ok))
            case _:
                return Result.Error(", ".join(errs))

type GetLinksFromHtmlInput = list[DataDto]

class GetLinksFromHtmlHandler(CustomActionHandler[GetLinksFromHtmlConfig, GetLinksFromHtmlInput]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("getlinksfromhtml")
    
    def validate_config(self, raw_config: dict[str, Any]) -> Result[GetLinksFromHtmlConfig, Any]:
        return GetLinksFromHtmlConfig.from_dict(raw_config)
    
    def validate_input(self, dto_list: list[DataDto]) -> Result[GetLinksFromHtmlInput, Any]:
        def from_dict(data: DataDto):
            content_res = parse_from_dict(data, "content", lambda content: content if isinstance(content, str) else None)
            return content_res.map(lambda _: data).default_value(None)
        data_list = [data for data in map(from_dict, dto_list) if data is not None]
        match data_list:
            case []:
                return Result.Error("html data is missing")
            case _:
                return Result.Ok(data_list)
    
    async def handle(self, config: GetLinksFromHtmlConfig, input: GetLinksFromHtmlInput) -> CompletedResult:
        def link_selector_to_dict(text_name: str, link_name: str, link: Selector):
            res = {
                text_name: link.css("a::text").get(),
                link_name: link.css("a::attr(href)").get()
            }
            return res
        @ex_to_error_result(Error.from_exception)
        def get_from_content(content: dict):
            content_selector = Selector(text=content["content"])
            links = content_selector.css("a")
            text_name = config.text_name or "text"
            link_name = config.link_name or "link"
            html_without_content = {k:v for k, v in content.items() if k != "content"}
            link_to_dict = functools.partial(link_selector_to_dict, text_name, link_name)
            res = [html_without_content | link_to_dict(link) for link in links]
            return res
        def ok_to_completed_result(result_data: list):
            return CompletedWith.Data(result_data) if result_data or config.return_empty_result else CompletedWith.NoData()
        def err_to_completed_result(err):
            return CompletedWith.Error(str(err))
        
        contents_res = traverse(get_from_content, Block(input))
        res = contents_res.map(lambda contents: functools.reduce(lambda acc, curr: acc + curr, contents, []))
        return res.map(ok_to_completed_result).default_with(err_to_completed_result)
