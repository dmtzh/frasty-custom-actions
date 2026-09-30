import asyncio
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
import functools
from typing import Any, override

from expression import Option, Result

from shared.action import Action, ActionName, ActionType
from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import Error, Metadata
from shared.infrastructure.storage.repository import NotFoundError
from shared.pipeline.actionhandler import ActionData, DataDto
from shared.utils.asyncresult import AsyncResult, coroutine_result
from shared.utils.parse import NonEmptyStr
from shared.utils.result import apply
from shared.validation import format_value_errors, ValueError as ValueErr, ValueMissing, ValueInvalid, with_prefix
from shared.utils.result import traverse_accumulating_with_index
from customactionhandler import ActionHandlerFactoryInput, RegistrableCustomActionHandler, named_action_handler

from .config import (
    SaveDataConfig,
    SaveDataConfigData,
    SaveDataConfigMetadata,
    SaveDataConfigStatic,
    WriteMode,
    parse_save_data_config,
)

# Type alias for a storage client function.
# A storage client is an async function that takes a string ID and data,
# and saves the data to the storage under the given ID.
# It should return Result to handle unexpected error cases without exceptions.
# The function is expected to be a closure that captures its own state
# (e.g., semaphore for concurrency limiting, connection config).
#
# IMPORTANT: The data parameter can be:
#   - A single DataDto (when id_source=DATA and data_field_name=None)
#   - A single value (when id_source=DATA and data_field_name is set)
#   - A list of DataDto (when id_source=METADATA and data_field_name=None)
#   - A list of values (when id_source=METADATA and data_field_name is set)
# The client is responsible for handling both single items and lists.
type SaveStorageClient = Callable[[str, Any], Coroutine[Any, Any, Result[None, Any]]]


# Type alias for the function that resolves a storage client by name.
# It takes a non empty string of storage client name and returns
# the associated client, or None if the client is not found.
# It should return Result to handle unexpected error cases without exceptions.
# This is the only dependency SaveDataHandler needs from the storage layer.
type SetDataFunc = Callable[[Any | None, Any], tuple[None, Any]]
type GetSaveClientFunc = Callable[[NonEmptyStr, SetDataFunc], Result[SaveStorageClient | None, Any]]


type SaveDataInput = list[DataDto]

@dataclass(frozen=True)
class IdWithError:
    id: str
    error: Any


def _ensure_list(value: Any) -> list:
    """
    Wrap a value in a list if it is not already a list.
    None becomes an empty list. Lists are returned as-is.
    All other types are wrapped in a single-element list.
    """
    match value:
        case None:
            return []
        case list():
            return value
        case _:
            return [value]


def _create_set_data_func(write_mode: WriteMode) -> SetDataFunc:
    """
    Create a SetDataFunc based on the configured write mode.
    REPLACE: ignores current state, returns new data as-is.
    APPEND:  concatenates [current + new], wrapping non-lists via _ensure_list.
    PREPEND: concatenates [new + current], wrapping non-lists via _ensure_list.
    """
    match write_mode:
        case WriteMode.REPLACE:
            return lambda _, data: (None, data)
        case WriteMode.APPEND:
            def append_func(current: Any | None, new: Any) -> tuple[None, Any]:
                return (None, _ensure_list(current) + _ensure_list(new))
            return append_func
        case WriteMode.PREPEND:
            def prepend_func(current: Any | None, new: Any) -> tuple[None, Any]:
                return (None, _ensure_list(new) + _ensure_list(current))
            return prepend_func


class SaveDataHandler(RegistrableCustomActionHandler[SaveDataConfig, SaveDataInput]):
    """
    Handler that saves data to an external storage by a string ID.

    Configuration is a discriminated union based on id_source:
      - SaveDataConfigData: ID from DataDto[id_field_name], with deduplication.
      - SaveDataConfigMetadata: ID from metadata[id_field_name], single ID for batch.
      - SaveDataConfigStatic: ID from config.fixed_id, static for this step.

    Each variant contains only semantically relevant fields. No conditional
    fields or runtime asserts needed — type system guarantees field availability.

    Supports three write modes (REPLACE, APPEND, PREPEND) across all variants.
    Saves in parallel via asyncio.gather (best-effort mode).
    On success, returns the original (non-deduplicated) input_list.
    Empty input_list skips the storage call entirely.
    """
    
    def __init__(self, get_client: GetSaveClientFunc) -> None:
        def _get_client(name: NonEmptyStr, set_data: SetDataFunc):
            opt_client_res = get_client(name, set_data).map(Option.of_optional)
            client_res = opt_client_res.bind(
                lambda opt_client: Result.of_option(opt_client, NotFoundError(name))
            )
            return client_res
        self._get_client = _get_client

    @override
    def to_action_handler_factory_input(self) -> ActionHandlerFactoryInput[SaveDataConfig, SaveDataInput]:
        @named_action_handler(self.action_name)
        def handle_wrapper(data: ActionData[SaveDataConfig, SaveDataInput]):
            """
            Applies ``validate_metadata`` before invoking ``handle``.
            """
            async def validate_metadata_err_to_completed_result(err: Any):
                return CompletedWith.Error(str(err))
            res = self.validate_metadata(data.config, data.metadata)\
                .map(lambda m: self.handle(data.config, data.input, m))\
                .map_error(validate_metadata_err_to_completed_result)\
                .merge()
            return res
        return ActionHandlerFactoryInput(
            action=Action(self.action_name, ActionType.CUSTOM),
            config_validator=self.validate_config,
            input_validator=self.validate_input,
            handler=handle_wrapper
        )
    
    @property
    def action_name(self) -> ActionName:
        return ActionName("savedata")
    
    def validate_config(self, raw_config: dict[str, Any]) -> Result[SaveDataConfig, Any]:
        return parse_save_data_config(raw_config).map_error(format_value_errors)
    
    def validate_input(
        self, config: SaveDataConfig, dto_list: list[DataDto]
    ) -> Result[SaveDataInput, Any]:
        """
        Fail-Fast validation of input data.
        Pattern matches on config type to guarantee field availability at type level.
        All errors include element index via with_prefix for precise error location.
        """
        def validate_id_field(dto: DataDto, id_field_name: NonEmptyStr) -> Result[None, ValueErr]:
            if id_field_name not in dto:
                return Result.Error(ValueMissing(id_field_name))
            else:
                id_value = dto[id_field_name]
                if not isinstance(id_value, str):
                    return Result.Error(ValueInvalid(id_field_name, id_value))
            return Result.Ok(None)

        def validate_data_field(dto: DataDto, data_field_name: NonEmptyStr | None) -> Result[None, ValueErr]:
            if data_field_name is None:
                return Result.Ok(None)
            if data_field_name not in dto:
                return Result.Error(ValueMissing(data_field_name))
            return Result.Ok(None)

        def validate_item_for_data(config: SaveDataConfigData, dto: DataDto) -> Result[None, tuple[ValueErr, ...]]:
            id_res = validate_id_field(dto, config.id_field_name)
            data_res = validate_data_field(dto, config.data_field_name)
            return apply(lambda _, __: None, lambda err: err, id_res, data_res)

        def validate_item_for_projection(dto: DataDto) -> Result[None, tuple[ValueErr, ...]]:
            return validate_data_field(dto, config.data_field_name).map_error(lambda err: tuple([err]))

        match config:
            case SaveDataConfigData():
                validator = functools.partial(validate_item_for_data, config)
            case SaveDataConfigMetadata() | SaveDataConfigStatic():
                validator = validate_item_for_projection

        prefixed_validator = lambda idx, dto: validator(dto)\
            .map_error(functools.partial(with_prefix, f"input_data[{idx}]"))

        return traverse_accumulating_with_index(dto_list, prefixed_validator)\
            .map(lambda _: dto_list)\
            .map_error(format_value_errors)
                
    
    def validate_metadata(
        self, config: SaveDataConfig, metadata: Metadata
    ) -> Result[Metadata, Any]:
        """
        Validate metadata when config is SaveDataConfigMetadata.
        """
        def validate_id_field(id_field_name: NonEmptyStr) -> Result[None, ValueErr]:
            if id_field_name not in metadata:
                return Result.Error(ValueMissing(id_field_name))
            else:
                id_value = metadata[id_field_name]
                if not isinstance(id_value, str):
                    return Result.Error(ValueInvalid(id_field_name, id_value))
            return Result.Ok(None)
        match config:
            case SaveDataConfigMetadata():
                id_res = validate_id_field(config.id_field_name)
                res = id_res.map(lambda _: metadata)\
                    .map_error(lambda err: tuple([err]))\
                    .map_error(functools.partial(with_prefix, "metadata"))\
                    .map_error(format_value_errors)
                return res
            case SaveDataConfigData() | SaveDataConfigStatic():
                return Result.Ok(metadata)

    
    async def handle(self, config: SaveDataConfig, input_list: SaveDataInput, metadata: Metadata) -> CompletedResult:
        @coroutine_result[Error]()
        async def save_data_workflow():
            # --- Step 1: Resolve the storage client with appropriate SetDataFunc ---
            set_data_func = _create_set_data_func(config.write_mode)
            client_res = self._get_client(config.storage_name, set_data_func)\
                .map_error(lambda err: Error(f"Get storage client error: {err}"))
            client = await AsyncResult.from_result(client_res)

            # --- Step 2: Form (id, data) pairs based on config type ---
            pairs: list[tuple[str, Any]] = []
            match config:
                case SaveDataConfigData():
                    # id_field_name guaranteed non-None by type
                    deduplicated: dict[str, DataDto] = {}
                    for dto in input_list:
                        deduplicated[dto[config.id_field_name]] = dto
                    for id_value, dto in deduplicated.items():
                        data = dto[config.data_field_name] if config.data_field_name is not None else dto
                        pairs.append((id_value, data))

                case SaveDataConfigMetadata():
                    # id_field_name guaranteed non-None by type
                    id_value = metadata[config.id_field_name]
                    if config.data_field_name is not None:
                        data = [dto[config.data_field_name] for dto in input_list]
                    else:
                        data = input_list
                    pairs = [(id_value, data)]

                case SaveDataConfigStatic():
                    # fixed_id guaranteed non-None by type
                    id_value = config.fixed_id
                    if config.data_field_name is not None:
                        data = [dto[config.data_field_name] for dto in input_list]
                    else:
                        data = input_list
                    pairs = [(id_value, data)]

            # Empty batch: skip storage call entirely
            if not pairs:
                return input_list

            # --- Step 3: Parallel save (best-effort via asyncio.gather) ---
            async def save_item(storage_client: SaveStorageClient, id_value: str, data: Any):
                res = await storage_client(id_value, data)
                return res.map_error(lambda err: IdWithError(id_value, err))

            tasks = [save_item(client, id_value, data) for id_value, data in pairs]
            results_res = await asyncio.gather(*tasks)

            # --- Step 4: Collect all errors (best-effort aggregation) ---
            initial_res = Result[None, tuple[IdWithError, ...]].Ok(None)
            final_result_res = functools.reduce(
                lambda acc, res: apply(lambda _, __: None, lambda err: err, acc, res),
                results_res,
                initial_res
            ).map_error(lambda err: Error(f"Failed to save data to storage '{config.storage_name}': {err}"))
            await AsyncResult.from_result(final_result_res)

            # --- Step 5: Return original input_list (not deduplicated) ---
            return input_list

        def ok_to_completed_result(result_list: list[DataDto]) -> CompletedResult:
            return CompletedWith.Data(result_list) if result_list or config.return_empty_result else CompletedWith.NoData()

        def err_to_completed_result(err: Error) -> CompletedResult:
            return CompletedWith.Error(str(err))

        save_data_res = await save_data_workflow()
        completed_result = save_data_res.map(ok_to_completed_result).default_with(err_to_completed_result)
        return completed_result
