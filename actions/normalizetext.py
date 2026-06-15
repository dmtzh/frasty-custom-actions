from dataclasses import dataclass
import functools
import re
from typing import Any

from expression import Result

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import Error
from shared.pipeline.actionhandler import DataDto
from shared.utils.exceptiondecorators import ex_to_error_result
from shared.utils.parse import parse_from_dict, parse_non_empty_str
from shared.utils.result import apply

from customactionhandler import CustomActionHandler

@ex_to_error_result(Error.from_exception)
def _normalize_text(input_dict: DataDto, field_name: str) -> DataDto:
    """
    Normalizes the value of a specific field in a dictionary by cleaning HTML tags 
    and whitespace, according to the defined architectural specifications.
    
    Args:
        input_dict: The dictionary containing the data.
        field_name: The exact name of the key whose value needs to be normalized.
        
    Returns:
        The modified dictionary with the normalized value for the specified field.
        
    Raises:
        TypeError: If the value associated with field_name exists but is not a string.
    """
    
    # 1. Check if the field exists. If absent or None, return the dictionary unchanged.
    if field_name not in input_dict or input_dict[field_name] is None:
        return input_dict
    
    # 2. Validate the type of the value. Raise TypeError if it is not a string.
    value = input_dict[field_name]
    if not isinstance(value, str):
        raise TypeError(
            f"Value for field_name '{field_name}' must be a string, "
            f"but got {type(value).__name__}"
        )
    
    text = value
    
    # 3.1. Remove whitespace strictly BETWEEN two closing HTML tags.
    # Example: "</p >  \n  </li >" becomes "</p ></li >"
    text = re.sub(r'(</[^>]+>)\s+(</[^>]+>)', r'\1\2', text, flags=re.IGNORECASE)
    
    # 3.2a. Replace the LAST </li> (the one immediately followed by </ul> or </ol>) with a dot and a space.
    # The positive lookahead (?=\s*</[uo]l>) ensures we only target the final list item.
    text = re.sub(r'</li\s*>(?=\s*</[uo]l>)', '. ', text, flags=re.IGNORECASE)
    
    # 3.2b. Replace all REMAINING </li> tags with a semicolon and a space.
    text = re.sub(r'</li\s*>', '; ', text, flags=re.IGNORECASE)
    
    # 3.3. Conditionally replace <br> variants with a dot and a space 
    # if followed by optional whitespace and an uppercase letter (Latin A-Z or Cyrillic А-ЯЁ).
    text = re.sub(r'<br\s*/?>(?=\s*[A-ZА-ЯЁ])', '. ', text, flags=re.IGNORECASE)
    
    # 3.4. Replace any remaining <br> variants with a single space.
    text = re.sub(r'<br\s*/?>', ' ', text, flags=re.IGNORECASE)
    
    # 3.5. Replace other closing block tags (like </p>) with a single space.
    text = re.sub(r'</p\s*>', ' ', text, flags=re.IGNORECASE)
    
    # 3.5a. NEW: Replace opening <ul> or <ol> tags with ': ' to properly introduce the list.
    text = re.sub(r'<[uo]l\s*>', ': ', text, flags=re.IGNORECASE)
    
    # 3.6. Remove all remaining HTML tags (opening tags, inline tags, </ul>, </ol>, etc.).
    text = re.sub(r'<[^>]+>', '', text)
    
    # 3.7. Typographic normalization: remove any whitespace immediately preceding a dot, semicolon, or colon.
    # This fixes artifacts like " ;", " .", or " :" into ";", ".", or ":"
    text = re.sub(r'\s+([.;:])', r'\1', text)
    
    # 3.7a. FIX PUNCTUATION ARTIFACTS: Replace any sequence of punctuation marks 
    # with the FIRST character of that sequence. 
    # Example: ",;" becomes ",", ".." becomes ".", ": :" becomes ":".
    # This preserves the original punctuation and discards artificially added duplicates.
    text = re.sub(r'([.,;:!?])[.,;:!?]+', r'\1', text)
    
    # 3.8. Collapse any sequence of whitespace characters into a single space.
    text = re.sub(r'\s+', ' ', text)
    
    # 3.9. Remove leading and trailing whitespace.
    text = text.strip()
    
    # 4. Assign the normalized string back to the dictionary.
    input_dict[field_name] = text
    
    return input_dict

@dataclass(frozen=True)
class NormalizeTextConfig:
    field_name: str

    @staticmethod
    def from_dict(data: dict[str, Any]):
        def validate_field_name() -> Result[str, str]:
            return parse_from_dict(data, "field_name", parse_non_empty_str)
        field_name_res = validate_field_name()
        return field_name_res.map(NormalizeTextConfig)

type NormalizeTextInput = list[DataDto]

class NormalizeTextHandler(CustomActionHandler[NormalizeTextConfig, NormalizeTextInput]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("normalizetext")
    
    def validate_config(self, raw_config: dict[str, Any]) -> Result[NormalizeTextConfig, Any]:
        return NormalizeTextConfig.from_dict(raw_config)
    
    def validate_input(self, _: NormalizeTextConfig, dto_list: list[DataDto]) -> Result[NormalizeTextInput, Any]:
        return Result.Ok(dto_list)
    
    async def handle(self, config: NormalizeTextConfig, input: NormalizeTextInput) -> CompletedResult:
        initial_res = Result[NormalizeTextInput, tuple[str, ...]].Ok([])
        def reduce_func(acc_output_res: Result[NormalizeTextInput, tuple[str, ...]], input: DataDto):
            input_res = _normalize_text(input, config.field_name).map_error(lambda err: err.message)
            return apply(lambda acc, def_with_id: [*acc, def_with_id], lambda err: err, acc_output_res, input_res)
        output_res = functools.reduce(reduce_func, input, initial_res)
        def err_to_completed_result(err):
            return CompletedWith.Error(str(err))
        return output_res.map(CompletedWith.Data).default_with(err_to_completed_result)
