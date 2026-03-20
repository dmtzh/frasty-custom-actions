from dataclasses import dataclass
from typing import Any
from expression import Result

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.pipeline.actionhandler import DataDto
from shared.utils.parse import parse_from_dict

from customactionhandler import CustomActionHandlerWithoutConfig

@dataclass(frozen=True)
class FilterJsonResponseInput:
    content_type: str
    content: str
    @staticmethod
    def from_dict(data: DataDto) -> Result['FilterJsonResponseInput', str]:
        content_type_res = parse_from_dict(data, "content_type", lambda content_type: content_type if isinstance(content_type, str) else None)
        content_res = parse_from_dict(data, "content", lambda content: content if isinstance(content, str) else None)
        errs = [err for err in [content_type_res.swap().default_value(None), content_res.swap().default_value(None)] if err is not None]
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
        return FilterJsonResponseInput.from_dict(dto_list[0])
    
    async def handle(self, input: FilterJsonResponseInput) -> CompletedResult:
        match input.content_type:
            case "application/json":
                return CompletedWith.Data(data={"content": input.content})
            case _:
                err_msg = f"Expected json response but got {input.content_type}"
                return CompletedWith.Error(err_msg)
