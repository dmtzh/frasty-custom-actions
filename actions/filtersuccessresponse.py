from dataclasses import dataclass
from typing import Any
from expression import Result

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.pipeline.actionhandler import DataDto

from customactionhandler import CustomActionHandlerWithoutConfig

@dataclass(frozen=True)
class FilterSuccessResponseInput:
    opt_status_code: int | None
    data: DataDto 
    @staticmethod
    def from_dict(data: DataDto):
        if "status_code" not in data:
            return FilterSuccessResponseInput(None, data)
        match data["status_code"]:
            case int() as status_code:
                return FilterSuccessResponseInput(status_code, data)
            case _:
                return FilterSuccessResponseInput(None, data)

class FilterSuccessResponseHandler(CustomActionHandlerWithoutConfig[list[FilterSuccessResponseInput]]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("filtersuccessresponse")
    
    def validate_input(self, dto_list: list[DataDto]) -> Result[list[FilterSuccessResponseInput], Any]:
        input_list = [FilterSuccessResponseInput.from_dict(data) for data in dto_list]
        return Result.Ok(input_list)
    
    async def handle(self, input_list: list[FilterSuccessResponseInput]) -> CompletedResult:
        def process_input(input: FilterSuccessResponseInput) -> DataDto | None:
            match input.opt_status_code:
                case 200:
                    return input.data
                case _:
                    return None
        
        result_data = [res for input in input_list if (res := process_input(input)) is not None]
        return CompletedWith.Data(result_data) if result_data else CompletedWith.NoData()
