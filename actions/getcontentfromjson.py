from dataclasses import dataclass
import functools
import json
from typing import Any

from expression import Result
from expression.collections.block import Block
from expression.extra.result.traversable import traverse
import jsonpath_ng as jp
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
class GetContentFromJsonConfig:
    query: str
    output_name: str
    return_empty_result: bool

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['GetContentFromJsonConfig', str]:
        def validate_query() -> Result[str, str]:
            return parse_from_dict(data, "query", parse_non_empty_str)
        def validate_output_name() -> Result[str, str]:
            if "output_name" not in data:
                return Result.Ok("content")
            return parse_from_dict(data, "output_name", parse_non_empty_str)
        def validate_return_empty_result() -> Result[bool, str]:
            if "return_empty_result" not in data:
                return Result.Ok(False)
            return parse_from_dict(data, "return_empty_result", parse_bool_str)
        query_res = validate_query()
        output_name_res = validate_output_name()
        return_empty_result_res = validate_return_empty_result()
        errs = to_error_list(query_res, output_name_res, return_empty_result_res)
        match errs:
            case []:
                return Result.Ok(GetContentFromJsonConfig(query_res.ok, output_name_res.ok, return_empty_result_res.ok))
            case _:
                return Result.Error(", ".join(errs))

type GetContentFromJsonInput = list[DataDto]

class GetContentFromJsonHandler(CustomActionHandler[GetContentFromJsonConfig, GetContentFromJsonInput]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("getcontentfromjson")
    
    def validate_config(self, raw_config: dict[str, Any]) -> Result[GetContentFromJsonConfig, Any]:
        return GetContentFromJsonConfig.from_dict(raw_config)
    
    def validate_input(self, dto_list: list[DataDto]) -> Result[GetContentFromJsonInput, Any]:
        def from_dict(data: DataDto):
            content_res = parse_from_dict(data, "content", lambda content: content if isinstance(content, str) else None)
            return content_res.map(lambda _: data)
        data_list = to_ok_list(*map(from_dict, dto_list))
        return Result.Ok(data_list)
    
    async def handle(self, config: GetContentFromJsonConfig, input: GetContentFromJsonInput) -> CompletedResult:
        def match_value_to_json(match_value):
            match match_value:
                case str():
                    return match_value
                case _:
                    return json.dumps(match_value)
        def query_get_all(dict_with_content: dict):
            content_obj = json.loads(dict_with_content["content"])
            jp_query = jpx.parse(config.query)
            matches = [match_value_to_json(match.value) for match in jp_query.find(content_obj)]
            return matches
        @ex_to_error_result(Error.from_exception)
        def get_from_content(dict_with_content: dict):
            matches = query_get_all(dict_with_content)
            dict_without_output_name = {k:v for k, v in dict_with_content.items() if k != config.output_name}
            output_list = [dict_without_output_name | {config.output_name: match} for match in matches]
            return output_list
        def ok_to_completed_result(result_data: list):
            return CompletedWith.Data(result_data) if result_data or config.return_empty_result else CompletedWith.NoData()
        def err_to_completed_result(err):
            return CompletedWith.Error(str(err))
        
        contents_res = traverse(get_from_content, Block(input))
        res = contents_res.map(lambda contents: functools.reduce(lambda acc, curr: acc + curr, contents, []))
        return res.map(ok_to_completed_result).default_with(err_to_completed_result)
