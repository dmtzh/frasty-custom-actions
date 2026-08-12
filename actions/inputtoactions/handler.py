from collections.abc import Callable
import functools
import re
from typing import Any

from expression import Result
import jmespath

from shared.action import ActionName
from shared.completedresult import CompletedWith, CompletedResult
from shared.customtypes import DefinitionIdValue
from shared.definition import DefinitionAdapter
from shared.pipeline.actionhandler import DataDto
from shared.utils.result import apply
from shared.validation import ValueMissing

from customactionhandler import CustomActionHandler

from .config import InputToActionsConfig

type InputToActionsInput = list[DataDto]

class JmesPathInputResolver:
    def __init__(self, input: DataDto) -> None:
        self._input = input

    def __call__(self, path: str) -> Any:
        # Construct a safe JMESPath expression by quoting each part of the path
        expression = ".".join(f'"{path_item.strip()}"' for path_item in path.split("."))
        return jmespath.search(expression, self._input)

class InputToActionsHandler(CustomActionHandler[InputToActionsConfig, InputToActionsInput]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("inputtoactions")

    def validate_config(self, raw_config: dict[str, Any]) -> Result[InputToActionsConfig, Any]:
        return InputToActionsConfig.from_dict(raw_config)

    def validate_input(self, _: InputToActionsConfig, dto_list: list[DataDto]) -> Result[InputToActionsInput, Any]:
        if not dto_list:
            return Result.Error(ValueMissing("input_data"))
        return Result.Ok(dto_list)

    async def handle(self, config: InputToActionsConfig, input: InputToActionsInput) -> CompletedResult:
        def resolve_input_to_actions(
            actions: list[DataDto],
            resolver: Callable[[str], Any],
            pattern: str = r'\{\{\s*([\w.]+)\s*\}\}'
        ) -> Any:
            """
            Recursively walks through actions, replacing {{param}} with results from resolver(param).
            Returns a new structure (immutable), original config is not modified.
            """
            compiled = re.compile(pattern)
            
            def _walk(node: Any) -> Any:
                if isinstance(node, dict):
                    return {k: _walk(v) for k, v in node.items()}
                elif isinstance(node, list):
                    return [_walk(item) for item in node]
                elif isinstance(node, str):
                    # Optimization: if the string is entirely a parameter, return the value as-is
                    stripped = node.strip()
                    if compiled.fullmatch(stripped):
                        param_name = stripped[2:-2].strip()
                        return resolver(param_name)

                    # Otherwise - substitution in text with conversion to str
                    def _replacer(match: re.Match) -> str:
                        match resolver(match.group(1)):
                            case None:
                                return "null"
                            case resolved_value:
                                return str(resolved_value)

                    return compiled.sub(_replacer, node)
                # Numbers, bools, null pass through unchanged
                return node
            
            return _walk(actions)
        def input_item_to_actions(input: DataDto):
            resolver = JmesPathInputResolver(input)
            return resolve_input_to_actions(config.actions, resolver)
        def input_item_to_definition_with_id(input: DataDto):
            raw_definition = input_item_to_actions(input)
            definition_res = DefinitionAdapter.from_list(raw_definition)
            return definition_res.map(lambda _: {
                "definition_id": DefinitionIdValue.new_id().to_value_with_checksum(),
                "definition": raw_definition
            }).map_error(str)
        def err_to_completed_result(err):
            return CompletedWith.Error(str(err))
        
        initial_res = Result[InputToActionsInput, tuple[str, ...]].Ok([])
        def reduce_func(acc_output_res: Result[InputToActionsInput, tuple[str, ...]], input: DataDto):
            input_res = input_item_to_definition_with_id(input)
            return apply(lambda acc, def_with_id: [*acc, def_with_id], lambda err: err, acc_output_res, input_res)
        output_res = functools.reduce(reduce_func, input, initial_res)
        return output_res.map(CompletedWith.Data).default_with(err_to_completed_result)
