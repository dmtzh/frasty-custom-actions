from dataclasses import dataclass
from typing import Any
from expression import Result

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.pipeline.actionhandler import DataDto
from shared.utils.parse import parse_from_dict

from customactionhandler import CustomActionHandlerWithoutConfig

@dataclass(frozen=True)
class FilterSuccessResponseInput:
    status_code: int
    data: DataDto 
    @staticmethod
    def from_dict(data: DataDto):
        status_code_res = parse_from_dict(data, "status_code", lambda status_code: status_code if isinstance(status_code, int) else None)
        input_res = status_code_res.map(lambda status_code: FilterSuccessResponseInput(status_code, data))
        return input_res

class FilterSuccessResponseHandler(CustomActionHandlerWithoutConfig[FilterSuccessResponseInput]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("filtersuccessresponse")
    
    def validate_input(self, dto_list: list[DataDto]) -> Result[FilterSuccessResponseInput, Any]:
        return FilterSuccessResponseInput.from_dict(dto_list[0])
    
    async def handle(self, input: FilterSuccessResponseInput) -> CompletedResult:
        match input.status_code:
            case 200:
                return CompletedWith.Data(data=input.data)
            case _:
                err_msg = f"Expected success response code (200) but got {input.status_code}"
                return CompletedWith.Error(err_msg)
