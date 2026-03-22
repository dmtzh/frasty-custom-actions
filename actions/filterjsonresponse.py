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
class FilterJsonResponseInput:
    content_type: str
    content: str
    @staticmethod
    def from_dict(data: DataDto) -> Result['FilterJsonResponseInput', str]:
        content_type_res = parse_from_dict(data, "content_type", lambda content_type: content_type if isinstance(content_type, str) else None)
        content_res = parse_from_dict(data, "content", lambda content: content if isinstance(content, str) else None)
        errs = to_error_list(content_type_res, content_res)
        match errs:
            case []:
                return Result.Ok(FilterJsonResponseInput(content_type_res.ok, content_res.ok))
            case _:
                return Result.Error(", ".join(errs))

class FilterJsonResponseHandler(CustomActionHandlerWithoutConfig[FilterJsonResponseInput]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("filterjsonresponse")
    
    def validate_input(self, dto_list: list[DataDto]) -> Result[FilterJsonResponseInput, Any]:
        if not dto_list:
            return Result.Error("input data is missing")
        data_res_list = list(map(FilterJsonResponseInput.from_dict, dto_list))
        data_list = to_ok_list(*data_res_list)
        match data_list:
            case []:
                errs = to_error_list(*data_res_list)
                return Result.Error(", ".join(errs))
            case _:
                return Result.Ok(data_list[0])
    
    async def handle(self, input: FilterJsonResponseInput) -> CompletedResult:
        match input.content_type:
            case "application/json":
                return CompletedWith.Data(data={"content": input.content})
            case _:
                err_msg = f"Expected json response but got {input.content_type}"
                return CompletedWith.Error(err_msg)
