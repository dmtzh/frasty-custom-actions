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
class FilterSuccessResponseInput:
    status_code: int
    data: DataDto 
    @staticmethod
    def from_dict(data: DataDto):
        status_code_res = parse_from_dict(data, "status_code", lambda status_code: status_code if isinstance(status_code, int) else None)
        input_res = status_code_res.map(lambda status_code: FilterSuccessResponseInput(status_code, data))
        return input_res

class FilterSuccessResponseHandler(CustomActionHandlerWithoutConfig[list[FilterSuccessResponseInput]]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("filtersuccessresponse")
    
    def validate_input(self, dto_list: list[DataDto]) -> Result[list[FilterSuccessResponseInput], Any]:
        if not dto_list:
            return Result.Error("input data is missing")
        data_res_list = [FilterSuccessResponseInput.from_dict(data) for data in dto_list]
        data_list = to_ok_list(*data_res_list)
        match data_list:
            case []:
                errs = to_error_list(*data_res_list)
                return Result.Error(", ".join(errs))
            case _:
                return Result.Ok(data_list)
    
    async def handle(self, input_list: list[FilterSuccessResponseInput]) -> CompletedResult:
        def process_input(input: FilterSuccessResponseInput) -> Result[DataDto, str]:
            match input.status_code:
                case 200:
                    return Result.Ok(input.data)
                case _:
                    err_msg = f"Expected success response code (200) but got {input.status_code}"
                    return Result.Error(err_msg)
        
        results = [process_input(input) for input in input_list]
        success_results = to_ok_list(*results)
        match success_results:
            case []:
                err_msgs = to_error_list(*results)
                return CompletedWith.Error(", ".join(err_msgs))
            case _:
                return CompletedWith.Data(success_results)