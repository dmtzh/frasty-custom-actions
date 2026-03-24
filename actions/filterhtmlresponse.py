from dataclasses import dataclass
from typing import Any
from expression import Result

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.pipeline.actionhandler import DataDto
from shared.utils.parse import parse_from_dict
from shared.utils.result import to_error_list, to_ok_list

from customactionhandler import CustomActionHandlerWithoutConfig

@dataclass(frozen=True)
class FilterHtmlResponseInput:
    content_type: str
    content: str
    @staticmethod
    def from_dict(data: DataDto) -> Result['FilterHtmlResponseInput', str]:
        content_type_res = parse_from_dict(data, "content_type", lambda content_type: content_type if isinstance(content_type, str) else None)
        content_res = parse_from_dict(data, "content", lambda content: content if isinstance(content, str) else None)
        errs = to_error_list(content_type_res, content_res)
        match errs:
            case []:
                return Result.Ok(FilterHtmlResponseInput(content_type_res.ok, content_res.ok))
            case _:
                return Result.Error(", ".join(errs))

class FilterHtmlResponseHandler(CustomActionHandlerWithoutConfig[list[FilterHtmlResponseInput]]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("filterhtmlresponse")
    
    def validate_input(self, dto_list: list[DataDto]) -> Result[list[FilterHtmlResponseInput], Any]:
        if not dto_list:
            return Result.Error("input data is missing")
        data_res_list = [FilterHtmlResponseInput.from_dict(data) for data in dto_list]
        data_list = to_ok_list(*data_res_list)
        match data_list:
            case []:
                errs = to_error_list(*data_res_list)
                return Result.Error(", ".join(errs))
            case _:
                return Result.Ok(data_list)
    
    async def handle(self, input_list: list[FilterHtmlResponseInput]) -> CompletedResult:
        def process_input(input: FilterHtmlResponseInput) -> Result[dict[str, str], str]:
            match input.content_type:
                case "text/html":
                    return Result.Ok({"content": input.content})
                case _:
                    err_msg = f"Expected html response but got {input.content_type}"
                    return Result.Error(err_msg)
        
        results = [process_input(input) for input in input_list]
        success_results = to_ok_list(*results)
        match success_results:
            case []:
                err_msgs = to_error_list(*results)
                return CompletedWith.Error(", ".join(err_msgs))
            case _:
                return CompletedWith.Data(success_results)
