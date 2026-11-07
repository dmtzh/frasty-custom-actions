from dataclasses import dataclass
from typing import Any, Optional
from expression import Result

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.pipeline.actionhandler import DataDto
from shared.utils.parse import NonEmptyStr

from customactionhandler import CustomActionHandlerWithoutConfig

@dataclass(frozen=True)
class FilterHtmlResponseInput:
    content_type: NonEmptyStr
    data: DataDto
    @staticmethod
    def from_dict(data: DataDto) -> Optional['FilterHtmlResponseInput']:
        if "content_type" not in data:
            return None
        match NonEmptyStr.parse(data["content_type"]):
            case None:
                return None
            case content_type:
                return FilterHtmlResponseInput(content_type, data)

class FilterHtmlResponseHandler(CustomActionHandlerWithoutConfig[list[FilterHtmlResponseInput]]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("filterhtmlresponse")
    
    def validate_input(self, dto_list: list[DataDto]) -> Result[list[FilterHtmlResponseInput], Any]:
        input_list = [res for data in dto_list if (res := FilterHtmlResponseInput.from_dict(data)) is not None]
        return Result.Ok(input_list)
    
    async def handle(self, input_list: list[FilterHtmlResponseInput]) -> CompletedResult:
        def process_input(input: FilterHtmlResponseInput) -> DataDto | None:
            match input.content_type:
                case "text/html":
                    return input.data
                case _:
                    return None

        result_data = [res for input in input_list if (res := process_input(input)) is not None]
        return CompletedWith.Data(result_data) if result_data else CompletedWith.NoData()
