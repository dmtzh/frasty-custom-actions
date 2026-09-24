from dataclasses import dataclass
import functools
import json
from typing import Any

from expression import Result

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import Error
from shared.pipeline.actionhandler import DataDto
from shared.utils.exceptiondecorators import ex_to_error_result
from shared.utils.parse import parse_bool_str, parse_dict_field
from shared.utils.result import apply, sequence_accumulating, to_ok_list, traverse_accumulating_with_index
from shared.validation import ValueError as ValueErr, format_value_errors

from customactionhandler import CustomActionHandler

from .getfromjson.config import GetFromJsonConfig, GetFromJsonOperationConfig
from .getfromjson.handler import GetFromJsonHandler

@dataclass(frozen=True)
class ContentToJsonConfig:
    operations: tuple[GetFromJsonOperationConfig, ...] | None
    return_empty_result: bool

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['ContentToJsonConfig', tuple[ValueErr, ...]]:
        def validate_operations() -> Result[tuple[GetFromJsonOperationConfig, ...] | None, tuple[ValueErr, ...]]:
            if "operations" not in data:
                return Result.Ok(None)
            return GetFromJsonConfig.from_dict(data).map(lambda config: config.operations)
        def validate_return_empty_result() -> Result[bool, ValueErr]:
            if "return_empty_result" not in data:
                return Result.Ok(False)
            return parse_dict_field(data, "return_empty_result", parse_bool_str)
        operations_res = validate_operations()
        return_empty_result_res = validate_return_empty_result()
        config_res = apply(ContentToJsonConfig, lambda errs: errs, operations_res, return_empty_result_res)
        return config_res

type ContentToJsonInput = list[DataDto]

class ContentToJsonHandler(CustomActionHandler[ContentToJsonConfig, ContentToJsonInput]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("contenttojson")
    
    def validate_config(self, raw_config: dict[str, Any]) -> Result[ContentToJsonConfig, Any]:
        return ContentToJsonConfig.from_dict(raw_config).map_error(format_value_errors)
    
    def validate_input(self, _: ContentToJsonConfig, dto_list: list[DataDto]) -> Result[ContentToJsonInput, Any]:
        if not dto_list:
            return Result.Error("input data is missing")

        def validate_item(idx: int, dto: DataDto) -> Result[DataDto, ValueErr]:
            return parse_dict_field(dto, "content", lambda v: v if isinstance(v, str) else None) \
                .map(lambda _: dto) \
                .map_error(lambda err: err.with_prefix(f"input_data[{idx}]"))

        return traverse_accumulating_with_index(dto_list, validate_item) \
            .map_error(format_value_errors)
    
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
                errs = sequence_accumulating(results).swap().default_value(tuple[Error, ...]())
                err_msgs = map(str, errs)
                return CompletedWith.Error(", ".join(err_msgs))
            case _:
                match config.operations:
                    case None:
                        return CompletedWith.Data(success_results)
                    case _:
                        get_from_json_config = GetFromJsonConfig(config.operations, config.return_empty_result)
                        return await GetFromJsonHandler().handle(get_from_json_config, success_results)