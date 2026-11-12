from dataclasses import dataclass
import functools
from typing import Any

from expression import Result
from parsel import Selector

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import Error
from shared.pipeline.actionhandler import DataDto
from shared.utils.exceptiondecorators import ex_to_error_result
from shared.utils.parse import DefaultValue, NonEmptyStr, parse_bool_str, parse_dict_field, parse_optional_default_value
from shared.utils.result import apply, traverse_accumulating_with_index
from shared.validation import format_value_errors, ValueError as ValueErr, ValueInvalid, ValueMissing

from customactionhandler import CustomActionHandler

class GetContentFromHtmlConfigSelector:
    class CssSelector:
        def __init__(self, css: NonEmptyStr):
            self._css = css
        def process_content(self, content) -> list[str]:
            return Selector(text=content).css(self._css).getall()
    
    class RegexSelector:
        def __init__(self, regex: NonEmptyStr):
            self._regex = regex
        def process_content(self, content) -> list[str]:
            return Selector(text=content).re(self._regex)

    class XPathSelector:
        def __init__(self, xpath: NonEmptyStr):
            self._xpath = xpath
        def process_content(self, content) -> list[str]:
            return Selector(text=content).xpath(self._xpath).getall()

    def __init__(self, selector: CssSelector | RegexSelector | XPathSelector, output_name: NonEmptyStr | None, default_value: DefaultValue | None):
        self._selector = selector
        self._output_name = output_name
        self._default_value = default_value

    def process_input(self, input: DataDto) -> list[DataDto]:
        content = input["content"]
        matches = self._selector.process_content(content)
        output_name = self._output_name or "content"
        match matches:
            case []:
                return [input | {output_name: self._default_value.value}] if self._default_value is not None else []
            case _:
                return [input | {output_name: match} for match in matches]
    
    @staticmethod
    def from_dict(data: dict) -> Result['GetContentFromHtmlConfigSelector', tuple[ValueErr, ...]]:
        def validate_css_selector() -> Result[GetContentFromHtmlConfigSelector.CssSelector, ValueErr] | None:
            if "css" not in data:
                return None
            return parse_dict_field(data, "css", NonEmptyStr.parse).map(GetContentFromHtmlConfigSelector.CssSelector)
        def validate_regex_selector() -> Result[GetContentFromHtmlConfigSelector.RegexSelector, ValueErr] | None:
            if "regex" not in data:
                return None
            return parse_dict_field(data, "regex", NonEmptyStr.parse).map(GetContentFromHtmlConfigSelector.RegexSelector)
        def validate_xpath_selector() -> Result[GetContentFromHtmlConfigSelector.XPathSelector, ValueErr] | None:
            if "xpath" not in data:
                return None
            return parse_dict_field(data, "xpath", NonEmptyStr.parse).map(GetContentFromHtmlConfigSelector.XPathSelector)
        def validate_output_name() -> Result[NonEmptyStr | None, ValueErr]:
            if "output_name" not in data:
                return Result.Ok(None)
            return parse_dict_field(data, "output_name", NonEmptyStr.parse)
        selector_res = validate_css_selector() or validate_regex_selector() or validate_xpath_selector() \
            or Result[GetContentFromHtmlConfigSelector.CssSelector, ValueErr].Error(ValueMissing("css/regex/xpath"))
        output_name_res = validate_output_name()
        opt_default_value = parse_optional_default_value(data)
        config_res = apply(lambda selector, output_name: GetContentFromHtmlConfigSelector(selector, output_name, opt_default_value), lambda errs: errs, selector_res, output_name_res)
        return config_res

@dataclass(frozen=True)
class GetContentFromHtmlConfig:
    selectors: tuple[GetContentFromHtmlConfigSelector, ...]
    return_empty_result: bool

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['GetContentFromHtmlConfig', tuple[ValueErr, ...]]:
        def validate_raw_selector(idx: int, raw_selector: Any) -> Result[GetContentFromHtmlConfigSelector, ValueErr] | Result[GetContentFromHtmlConfigSelector, tuple[ValueErr, ...]]:
            match raw_selector:
                case dict():
                    return GetContentFromHtmlConfigSelector.from_dict(raw_selector)
                case _:
                    return Result[GetContentFromHtmlConfigSelector, ValueErr].Error(
                        ValueInvalid(f"selectors[{idx}]", raw_selector)
                    )
        def validate_raw_selectors(raw_selectors: list[Any]) -> Result[tuple[GetContentFromHtmlConfigSelector, ...], tuple[ValueErr, ...]]:
            selectors_res = traverse_accumulating_with_index(
                raw_selectors, validate_raw_selector
            ).map(tuple)
            return selectors_res
        def validate_selectors() -> Result[tuple[GetContentFromHtmlConfigSelector, ...], tuple[ValueErr, ...]]:
            raw_selectors_res = parse_dict_field(data, "selectors", lambda selectors: selectors if isinstance(selectors, list) and selectors else None)\
                .map_error(lambda err: tuple([err]))
            return raw_selectors_res.bind(validate_raw_selectors)
        def validate_return_empty_result() -> Result[bool, ValueErr]:
            if "return_empty_result" not in data:
                return Result.Ok(False)
            return parse_dict_field(data, "return_empty_result", parse_bool_str)
        
        selectors_res = validate_selectors()
        return_empty_result_res = validate_return_empty_result()
        config_res = apply(GetContentFromHtmlConfig, lambda errs: errs, selectors_res, return_empty_result_res)
        return config_res

type GetContentFromHtmlInput = list[DataDto]

class GetContentFromHtmlHandler(CustomActionHandler[GetContentFromHtmlConfig, GetContentFromHtmlInput]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("getcontentfromhtml")
    
    def validate_config(self, raw_config: dict[str, Any]) -> Result[GetContentFromHtmlConfig, Any]:
        return GetContentFromHtmlConfig.from_dict(raw_config).map_error(format_value_errors)
    
    def validate_input(self, _: GetContentFromHtmlConfig, dto_list: list[DataDto]) -> Result[GetContentFromHtmlInput, Any]:
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

        initial_res = Result[GetContentFromHtmlInput, Error].Ok(input_list)
        res = functools.reduce(lambda acc_res, selector: acc_res.bind(lambda acc: get_content_from_input_list(acc, selector)), config.selectors, initial_res)
        return res.map(ok_to_completed_result).default_with(err_to_completed_result)
        