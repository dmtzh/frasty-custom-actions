from dataclasses import dataclass
import json
from typing import Any

from expression import Result

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import Error
from shared.pipeline.actionhandler import DataDto
from shared.utils.asyncresult import ex_to_error_result
from shared.utils.parse import parse_bool_str, parse_from_dict
from shared.utils.result import to_ok_list, to_error_list

from customactionhandler import CustomActionHandler
from .getfromjson import GetFromJsonConfig, GetFromJsonConfigOperation, GetFromJsonHandler

@dataclass(frozen=True)
class ContentToJsonConfig:
    operations: tuple[GetFromJsonConfigOperation, ...] | None
    return_empty_result: bool

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['ContentToJsonConfig', str]:
        def validate_operations() -> Result[tuple[GetFromJsonConfigOperation, ...] | None, str]:
            if "operations" not in data:
                return Result.Ok(None)
            return GetFromJsonConfig.from_dict(data).map(lambda config: config.operations)
        def validate_return_empty_result() -> Result[bool, str]:
            if "return_empty_result" not in data:
                return Result.Ok(False)
            return parse_from_dict(data, "return_empty_result", parse_bool_str)
        operations_res = validate_operations()
        return_empty_result_res = validate_return_empty_result()
        errs = to_error_list(operations_res, return_empty_result_res)
        match errs:
            case []:
                return Result.Ok(ContentToJsonConfig(operations_res.ok, return_empty_result_res.ok))
            case _:
                return Result.Error(", ".join(errs))

type ContentToJsonInput = list[DataDto]

class ContentToJsonHandler(CustomActionHandler[ContentToJsonConfig, ContentToJsonInput]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("contenttojson")
    
    def validate_config(self, raw_config: dict[str, Any]) -> Result[ContentToJsonConfig, Any]:
        return ContentToJsonConfig.from_dict(raw_config)
    
    def validate_input(self, dto_list: list[DataDto]) -> Result[ContentToJsonInput, Any]:
        if not dto_list:
            return Result.Error("input data is missing")
        def from_dict(data: DataDto):
            content_res = parse_from_dict(data, "content", lambda content: content if isinstance(content, str) else None)
            return content_res.map(lambda _: data)
        data_list = to_ok_list(*map(from_dict, dto_list))
        if not data_list:
            return Result.Error("'content' key is missing")
        return Result.Ok(data_list)
    
    async def handle(self, config: ContentToJsonConfig, input_list: ContentToJsonInput) -> CompletedResult:
        @ex_to_error_result(Error.from_exception)
        def content_to_json(dict_with_content: DataDto):
            json_content = json.loads(dict_with_content["content"])
            dict_without_content = {k:v for k, v in dict_with_content.items() if k != "content"}
            return dict_without_content | {"content": json_content}
        
        results = [content_to_json(input) for input in input_list]
        success_results = to_ok_list(*results)
        match success_results:
            case []:
                err_msgs = map(str, to_error_list(*results))
                return CompletedWith.Error(", ".join(err_msgs))
            case _:
                match config.operations:
                    case None:
                        return CompletedWith.Data(success_results)
                    case _:
                        get_from_json_config = GetFromJsonConfig(config.operations, config.return_empty_result)
                        return await GetFromJsonHandler().handle(get_from_json_config, success_results)