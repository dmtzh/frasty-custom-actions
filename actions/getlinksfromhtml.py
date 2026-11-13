from dataclasses import dataclass
import functools
import itertools
from typing import Any, Optional

from expression import Result
from parsel import Selector

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import Error
from shared.pipeline.actionhandler import DataDto
from shared.utils.exceptiondecorators import ex_to_error_result
from shared.utils.parse import NonEmptyStr, parse_bool_str, parse_dict_field
from shared.utils.result import apply3, traverse_accumulating_with_index
from shared.validation import format_value_errors, ValueError as ValueErr

from customactionhandler import CustomActionHandler

@dataclass(frozen=True)
class GetLinksFromHtmlConfig:
    text_name: NonEmptyStr | None
    link_name: NonEmptyStr | None
    return_empty_result: bool

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['GetLinksFromHtmlConfig', tuple[ValueErr, ...]]:
        def validate_text_name() -> Result[NonEmptyStr | None, ValueErr]:
            if "text_name" not in data:
                return Result.Ok(None)
            return parse_dict_field(data, "text_name", NonEmptyStr.parse)
        def validate_link_name() -> Result[NonEmptyStr | None, ValueErr]:
            if "link_name" not in data:
                return Result.Ok(None)
            return parse_dict_field(data, "link_name", NonEmptyStr.parse)
        def validate_return_empty_result() -> Result[bool, ValueErr]:
            if "return_empty_result" not in data:
                return Result.Ok(False)
            return parse_dict_field(data, "return_empty_result", parse_bool_str)
        text_name_res = validate_text_name()
        link_name_res = validate_link_name()
        return_empty_result_res = validate_return_empty_result()
        config_res = apply3(GetLinksFromHtmlConfig, lambda errs: errs, text_name_res, link_name_res, return_empty_result_res)
        return config_res

@dataclass(frozen=True)
class GetLinksFromHtmlInput:
    content: str
    data_without_content: DataDto
    @staticmethod
    def from_dict(data: DataDto) -> Optional['GetLinksFromHtmlInput']:
        if "content" not in data:
            return None
        match data["content"]:
            case str()  as content:
                data_without_content = {k:v for k, v in data.items() if k != "content"}
                return GetLinksFromHtmlInput(content, data_without_content)
            case _:
                return None

class GetLinksFromHtmlHandler(CustomActionHandler[GetLinksFromHtmlConfig, list[GetLinksFromHtmlInput]]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("getlinksfromhtml")
    
    def validate_config(self, raw_config: dict[str, Any]) -> Result[GetLinksFromHtmlConfig, Any]:
        return GetLinksFromHtmlConfig.from_dict(raw_config).map_error(format_value_errors)
    
    def validate_input(self, _: GetLinksFromHtmlConfig, dto_list: list[DataDto]) -> Result[list[GetLinksFromHtmlInput], Any]:
        input_list = [res for data in dto_list if (res := GetLinksFromHtmlInput.from_dict(data)) is not None]
        return Result.Ok(input_list)
    
    async def handle(self, config: GetLinksFromHtmlConfig, input_list: list[GetLinksFromHtmlInput]) -> CompletedResult:
        def link_selector_to_dict(text_name: str, link_name: str, link: Selector):
            res = {
                text_name: link.css("a::text").get(),
                link_name: link.css("a::attr(href)").get()
            }
            return res
        @ex_to_error_result(Error.from_exception)
        def get_from_content(item: GetLinksFromHtmlInput):
            content_selector = Selector(text=item.content)
            links = content_selector.css("a")
            text_name = config.text_name or "text"
            link_name = config.link_name or "link"
            link_to_dict = functools.partial(link_selector_to_dict, text_name, link_name)
            res = [item.data_without_content | link_to_dict(link) for link in links]
            return res
        prefixed_get_from_content = lambda idx, item: get_from_content(item)\
            .map_error(lambda err: Error(f"input_data[{idx}].content: {err.message}"))
        def ok_to_completed_result(result_data: list):
            return CompletedWith.Data(result_data) if result_data or config.return_empty_result else CompletedWith.NoData()
        def err_to_completed_result(err):
            return CompletedWith.Error(str(err))

        with_links_res = traverse_accumulating_with_index(input_list, prefixed_get_from_content)
        res = with_links_res.map(lambda items: list(itertools.chain.from_iterable(items)))
        return res.map(ok_to_completed_result).default_with(err_to_completed_result)
