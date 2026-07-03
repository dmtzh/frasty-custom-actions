from dataclasses import dataclass
from enum import StrEnum
import functools
import json
import re
from typing import Any

from expression import Result
from expression.collections.block import Block

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import Error
from shared.pipeline.actionhandler import DataDto
from shared.utils.exceptiondecorators import ex_to_error_result
from shared.utils.parse import parse_bool_str, parse_from_dict, parse_non_empty_str
from shared.utils.result import apply, apply4
from shared.utils.string import strip_and_lowercase
from customactionhandler import CustomActionHandler


class MissingKeyStrategy(StrEnum):
    """Strategy for handling missing template variables."""
    FAIL = "fail"
    LEAVE_AS_IS = "leave_as_is"
    REPLACE_WITH_EMPTY = "replace_with_empty"

    @staticmethod
    def parse(value: str) -> 'MissingKeyStrategy | None':
        """Parse a string into a MissingKeyStrategy enum value."""
        if value is None:
            return None
        match strip_and_lowercase(value):
            case MissingKeyStrategy.FAIL.value:
                return MissingKeyStrategy.FAIL
            case MissingKeyStrategy.LEAVE_AS_IS.value:
                return MissingKeyStrategy.LEAVE_AS_IS
            case MissingKeyStrategy.REPLACE_WITH_EMPTY.value:
                return MissingKeyStrategy.REPLACE_WITH_EMPTY
            case _:
                return None


class TemplateContextResolver:
    """
    Resolves template variable paths against a context composed of DataDto and global variables.
    DataDto has priority over global_variables in case of key collision.
    """

    def __init__(self, data_dto: DataDto, global_variables: dict[str, Any] | None) -> None:
        self._data_dto = data_dto
        self._global_variables = global_variables or {}

    def __call__(self, path: str) -> Any:
        """
        Resolve a dotted path (e.g., 'user.profile.name') against the context.
        Returns the resolved value, or None if not found.
        Distinguishes between 'key not found' and 'key exists with None value'.
        """
        parts = [p.strip() for p in path.split(".")]

        # Try DataDto first (priority)
        data_value, data_found = self._resolve_path(self._data_dto, parts)
        if data_found:
            return data_value

        # Fallback to global_variables
        global_value, global_found = self._resolve_path(self._global_variables, parts)
        if global_found:
            return global_value

        # Not found anywhere
        return None

    @staticmethod
    def _resolve_path(context: dict[str, Any], parts: list[str]) -> tuple[Any, bool]:
        """
        Resolve a dotted path in a nested dictionary.
        Returns (value, found) where found indicates if the path exists.
        """
        current = context
        for part in parts:
            if not isinstance(current, dict) or part not in current:
                return None, False
            current = current[part]
        return current, True


def _convert_value(value: Any) -> str:
    """
    Convert a resolved template variable value to a string.
    - str: used as-is
    - bool: str() (must be checked before int, since bool is subclass of int)
    - int, float: str()
    - list, dict: json.dumps with ensure_ascii=False
    - other: str()
    """
    match value:
        case str():
            return value
        case bool():
            return str(value)
        case int() | float():
            return str(value)
        case list() | dict():
            return json.dumps(value, ensure_ascii=False)
        case _:
            return str(value)


# Compiled regex pattern for template variables: {{name}} or {{ name }} or {{user.profile.name}}
_TEMPLATE_PATTERN = re.compile(r'\{\{\s*([\w.]+)\s*\}\}')


@ex_to_error_result(Error.from_exception)
def _render_template(input_dict: DataDto, config: 'RenderTemplateConfig') -> DataDto:
    """
    Render template variables in the specified field of a DataDto.
    
    Args:
        input_dict: The dictionary containing the data.
        config: The render configuration.
    
    Returns:
        A new dictionary with the rendered field value.
    
    Raises:
        TypeError: If the field value exists but is not a string.
        ValueError: If a template variable is missing and on_missing=FAIL.
    """
    # 1. Check if the field exists. If absent, return the dictionary unchanged.
    if config.field_name not in input_dict:
        return input_dict

    # 2. Validate the type of the value. Raise TypeError if it is not a string.
    value = input_dict[config.field_name]
    if not isinstance(value, str):
        raise TypeError(
            f"Value for field '{config.field_name}' must be a string, "
            f"but got {type(value).__name__}"
        )

    # 3. Create resolver for this specific DataDto
    resolver = TemplateContextResolver(input_dict, config.global_variables)

    # 4. Replace template variables using re.sub with a callable (safe from regex injection)
    def replacer(match: re.Match) -> str:
        var_name = match.group(1)
        resolved_value = resolver(var_name)

        # Handle None as missing value
        if resolved_value is None:
            match config.on_missing:
                case MissingKeyStrategy.FAIL:
                    raise ValueError(f"missing variable '{var_name}'")
                case MissingKeyStrategy.LEAVE_AS_IS:
                    return match.group(0)  # Return original {{var}}
                case MissingKeyStrategy.REPLACE_WITH_EMPTY:
                    return ""

        # Convert value to string
        return _convert_value(resolved_value)

    rendered_text = _TEMPLATE_PATTERN.sub(replacer, value)

    # 5. Return new dict (immutable, do not mutate input)
    return {**input_dict, config.field_name: rendered_text}


@dataclass(frozen=True)
class RenderTemplateConfig:
    """Configuration for the RenderTemplateHandler."""
    field_name: str
    on_missing: MissingKeyStrategy
    global_variables: dict[str, Any] | None
    return_empty_result: bool

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['RenderTemplateConfig', str]:
        """Parse and validate configuration from a dictionary."""
        def validate_field_name() -> Result[str, str]:
            if "field_name" not in data:
                return Result.Ok("content")
            return parse_from_dict(data, "field_name", parse_non_empty_str)

        def validate_on_missing() -> Result[MissingKeyStrategy, str]:
            return parse_from_dict(data, "on_missing", MissingKeyStrategy.parse)

        def validate_global_variables() -> Result[dict[str, Any] | None, str]:
            if "global_variables" not in data:
                return Result.Ok(None)
            raw = data["global_variables"]
            if not isinstance(raw, dict):
                return Result.Error("invalid 'global_variables' value")
            return Result.Ok(raw)

        def validate_return_empty_result() -> Result[bool, str]:
            if "return_empty_result" not in data:
                return Result.Ok(False)
            return parse_from_dict(data, "return_empty_result", parse_bool_str)

        field_name_res = validate_field_name()
        on_missing_res = validate_on_missing()
        global_variables_res = validate_global_variables()
        return_empty_result_res = validate_return_empty_result()

        # Combine all results
        return apply4(RenderTemplateConfig, ", ".join, field_name_res, on_missing_res, global_variables_res, return_empty_result_res)

type RenderTemplateInput = list[DataDto]

class RenderTemplateHandler(CustomActionHandler[RenderTemplateConfig, RenderTemplateInput]):
    """
    Handler for rendering template variables in text fields.
    Replaces {{variable_name}} placeholders with values from DataDto or global_variables.
    """

    @property
    def action_name(self) -> ActionName:
        return ActionName("rendertemplate")

    def validate_config(self, raw_config: dict[str, Any]) -> Result[RenderTemplateConfig, Any]:
        return RenderTemplateConfig.from_dict(raw_config)

    def validate_input(self, _: RenderTemplateConfig, dto_list: list[DataDto]) -> Result[RenderTemplateInput, Any]:
        return Result.Ok(dto_list)

    async def handle(self, config: RenderTemplateConfig, input_list: RenderTemplateInput) -> CompletedResult:
        def ok_to_completed_result(result_block: Block[DataDto]) -> CompletedResult:
            """Materialize Block to list and wrap in CompletedResult."""
            result_list = list(result_block)
            return CompletedWith.Data(result_list) if result_list or config.return_empty_result else CompletedWith.NoData()

        def err_to_completed_result(err: tuple[Error, ...]) -> CompletedResult:
            """Convert error tuple to CompletedWith.Error."""
            return CompletedWith.Error(str(err))

        # Initialize accumulator with empty Block (O(1) append, immutable)
        initial_res = Result[Block[DataDto], tuple[Error, ...]].Ok(Block.empty())

        def reduce_func(
            acc_output_res: Result[Block[DataDto], tuple[Error, ...]],
            input_item: DataDto
        ) -> Result[Block[DataDto], tuple[Error, ...]]:
            """Process single item and accumulate result."""
            input_res = _render_template(input_item, config)
            return apply(
                lambda acc, resolved_item: acc.append(Block[DataDto].singleton(resolved_item)),
                lambda err: err,
                acc_output_res,
                input_res
            )

        # Process all items with fail-fast semantics
        res = functools.reduce(reduce_func, input_list, initial_res)
        return res.map(ok_to_completed_result).default_with(err_to_completed_result)
