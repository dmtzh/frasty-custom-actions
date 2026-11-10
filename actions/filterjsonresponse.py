from dataclasses import dataclass
from typing import Any, Optional
from expression import Result

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.pipeline.actionhandler import DataDto
from shared.utils.parse import NonEmptyStr

from customactionhandler import CustomActionHandlerWithoutConfig

@dataclass(frozen=True)
class FilterJsonResponseInput:
    content_type: NonEmptyStr
    data: DataDto
    @staticmethod
    def from_dict(data: DataDto) -> Optional['FilterJsonResponseInput']:
        if "content_type" not in data:
            return None
        match NonEmptyStr.parse(data["content_type"]):
            case None:
                return None
            case content_type:
                return FilterJsonResponseInput(content_type, data)

class FilterJsonResponseHandler(CustomActionHandlerWithoutConfig[list[FilterJsonResponseInput]]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("filterjsonresponse")

    def validate_input(self, dto_list: list[DataDto]) -> Result[list[FilterJsonResponseInput], Any]:
        input_list = [res for data in dto_list if (res := FilterJsonResponseInput.from_dict(data)) is not None]
        return Result.Ok(input_list)

    async def handle(self, input_list: list[FilterJsonResponseInput]) -> CompletedResult:
        def process_input(input: FilterJsonResponseInput) -> DataDto | None:
            match input.content_type:
                case "application/json":
                    return input.data
                case _:
                    return None

        result_data = [res for input in input_list if (res := process_input(input)) is not None]
        return CompletedWith.Data(result_data) if result_data else CompletedWith.NoData()
