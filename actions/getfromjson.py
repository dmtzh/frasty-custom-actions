from dataclasses import dataclass
import functools
from typing import Any

from expression import Result
import jsonpath_ng.ext as jpx

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import Error
from shared.pipeline.actionhandler import DataDto
from shared.utils.asyncresult import ex_to_error_result
from shared.utils.parse import parse_bool_str, parse_from_dict, parse_non_empty_str
from shared.utils.result import to_ok_list, to_error_list

from customactionhandler import CustomActionHandler

@dataclass(frozen=True)
class GetFromJsonConfigSelector:
    query: str
    output_name: str | None
    default_value: Any | None

    @staticmethod
    def from_dict(data: dict) -> Result['GetFromJsonConfigSelector', str]:
        def validate_query() -> Result[str, str]:
            return parse_from_dict(data, "query", parse_non_empty_str)
        def validate_output_name() -> Result[str | None, str]:
            if "output_name" not in data:
                return Result.Ok(None)
            return parse_from_dict(data, "output_name", parse_non_empty_str)
        query_res = validate_query()
        output_name_res = validate_output_name()
        default_value = data.get("default_value")
        errs = to_error_list(query_res, output_name_res)
        match errs:
            case []:
                return Result.Ok(GetFromJsonConfigSelector(query_res.ok, output_name_res.ok, default_value))
            case _:
                return Result.Error(", ".join(errs))

@dataclass(frozen=True)
class GetFromJsonConfig:
    selectors: tuple[GetFromJsonConfigSelector, ...]
    return_empty_result: bool

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['GetFromJsonConfig', str]:
        def validate_raw_selector(raw_selector) -> Result[GetFromJsonConfigSelector, str]:
            match raw_selector:
                case dict():
                    return GetFromJsonConfigSelector.from_dict(raw_selector)
                case _:
                    return Result.Error(f"invalid 'selector' value {raw_selector}")    
        def validate_raw_selectors(raw_selectors: list[Any]) -> Result[tuple[GetFromJsonConfigSelector, ...], str]:
            selectors_res_list = [validate_raw_selector(raw_selector) for raw_selector in raw_selectors]
            selectors_list = to_ok_list(*selectors_res_list)
            match selectors_list:
                case []:
                    errs = to_error_list(*selectors_res_list)
                    return Result.Error(", ".join(errs))
                case _:
                    return Result.Ok(tuple(selectors_list))
        def validate_selectors() -> Result[tuple[GetFromJsonConfigSelector, ...], str]:
            raw_selectors_res = parse_from_dict(data, "selectors", lambda selectors: selectors if isinstance(selectors, list) and selectors else None)
            selectors_res = raw_selectors_res.bind(validate_raw_selectors)
            errs = to_error_list(selectors_res)
            match errs:
                case []:
                    return Result.Ok(selectors_res.ok)
                case _:
                    return Result.Error(", ".join(errs))
        def validate_return_empty_result() -> Result[bool, str]:
            if "return_empty_result" not in data:
                return Result.Ok(False)
            return parse_from_dict(data, "return_empty_result", parse_bool_str)
        selectors_res = validate_selectors()
        return_empty_result_res = validate_return_empty_result()
        errs = to_error_list(selectors_res, return_empty_result_res)
        match errs:
            case []:
                return Result.Ok(GetFromJsonConfig(selectors_res.ok, return_empty_result_res.ok))
            case _:
                return Result.Error(", ".join(errs))

type GetFromJsonInput = list[DataDto]

class GetFromJsonHandler(CustomActionHandler[GetFromJsonConfig, GetFromJsonInput]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("getfromjson")
    
    def validate_config(self, raw_config: dict[str, Any]) -> Result[GetFromJsonConfig, Any]:
        return GetFromJsonConfig.from_dict(raw_config)
    
    def validate_input(self, dto_list: list[DataDto]) -> Result[GetFromJsonInput, Any]:
        return Result.Ok(dto_list)
    
    async def handle(self, config: GetFromJsonConfig, input_list: GetFromJsonInput) -> CompletedResult:
        def match_value_to_result(match_value, default_value):
            match match_value:
                case None if default_value is not None:
                    return default_value
                case _:
                    return match_value
        def query_get_all(input, selector: GetFromJsonConfigSelector):
            jp_query = jpx.parse(selector.query)
            matches = [match_value_to_result(match.value, selector.default_value) for match in jp_query.find(input)]
            return matches
        def get_from_input(input, selector: GetFromJsonConfigSelector):
            matches = query_get_all(input, selector)
            match selector.output_name:
                case None:
                    return matches
                case output_name:
                    dict_without_output_name = {k:v for k, v in input.items() if k != output_name}
                    output_list = [dict_without_output_name | {output_name: match} for match in matches]
                    return output_list
        @ex_to_error_result(Error.from_exception)
        def get_from_input_list(input_list, selector: GetFromJsonConfigSelector):
            return functools.reduce(lambda acc, curr: acc + get_from_input(curr, selector), input_list, [])
        def ok_to_completed_result(result_data: list):
            return CompletedWith.Data(result_data) if result_data or config.return_empty_result else CompletedWith.NoData()
        def err_to_completed_result(err):
            return CompletedWith.Error(str(err))
        
        res = functools.reduce(lambda acc_res, selector: acc_res.bind(lambda acc: get_from_input_list(acc, selector)), config.selectors, Result.Ok(input_list))
        return res.map(ok_to_completed_result).default_with(err_to_completed_result)