from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Optional

from expression import Result

from shared.utils.parse import NonEmptyStr, parse_bool_str, parse_dict_field
from shared.utils.result import apply, apply4
from shared.utils.string import strip_and_lowercase
from shared.validation import ValueError as ValueErr, ValueInvalid


class IdSource(StrEnum):
    """
    Specifies where the ID value should be extracted from.
    DATA     - ID is taken from each DataDto[id_field_name]
    METADATA - ID is taken from metadata[id_field_name] (single ID for the whole batch)
    CONFIG   - ID is taken from config.fixed_id (static ID, fixed for this step)
    """
    DATA = "data"
    METADATA = "metadata"
    CONFIG = "config"

    @staticmethod
    def parse(value: str) -> Optional["IdSource"]:
        if value is None:
            return None
        match strip_and_lowercase(value):
            case IdSource.DATA.value:
                return IdSource.DATA
            case IdSource.METADATA.value:
                return IdSource.METADATA
            case IdSource.CONFIG.value:
                return IdSource.CONFIG
            case _:
                return None


class WriteMode(StrEnum):
    """
    Specifies how data should be written to storage.
    REPLACE - Overwrite existing data completely (default).
    APPEND  - Append new data to the end of existing list. Non-list values are wrapped in a list.
    PREPEND - Prepend new data to the beginning of existing list. Non-list values are wrapped in a list.
    """
    REPLACE = "replace"
    APPEND = "append"
    PREPEND = "prepend"

    @staticmethod
    def parse(value: str) -> Optional["WriteMode"]:
        """
        Safely parse a string into a WriteMode enum value.
        Args:
            value: The string to parse (case-insensitive, whitespace-trimmed).
        Returns:
            The corresponding WriteMode value, or None if the string is not valid.
        """
        if value is None:
            return None
        match strip_and_lowercase(value):
            case WriteMode.REPLACE.value:
                return WriteMode.REPLACE
            case WriteMode.APPEND.value:
                return WriteMode.APPEND
            case WriteMode.PREPEND.value:
                return WriteMode.PREPEND
            case _:
                return None


@dataclass(frozen=True)
class SaveDataConfigData:
    """ID from DataDto[id_field_name]. Deduplication enabled."""
    storage_name: NonEmptyStr
    id_field_name: NonEmptyStr          # always required, never None
    write_mode: WriteMode
    data_field_name: NonEmptyStr | None
    return_empty_result: bool


@dataclass(frozen=True)
class SaveDataConfigMetadata:
    """ID from metadata[id_field_name]. Single ID for whole batch."""
    storage_name: NonEmptyStr
    id_field_name: NonEmptyStr          # always required, never None
    write_mode: WriteMode
    data_field_name: NonEmptyStr | None
    return_empty_result: bool


@dataclass(frozen=True)
class SaveDataConfigStatic:
    """ID from config.fixed_id. Static, fixed for this step."""
    storage_name: NonEmptyStr
    fixed_id: NonEmptyStr               # always required, never None
    write_mode: WriteMode
    data_field_name: NonEmptyStr | None
    return_empty_result: bool


type SaveDataConfig = SaveDataConfigData | SaveDataConfigMetadata | SaveDataConfigStatic


def _parse_common_fields(data: dict[str, Any]) -> Result[tuple[NonEmptyStr, WriteMode, NonEmptyStr | None, bool], tuple[ValueErr, ...]]:
    """
    Parse fields common to all SaveDataConfig variants.
    Returns tuple (storage_name, write_mode, data_field_name, return_empty_result).
    All four fields are independent and validated in parallel via apply4.
    """
    storage_name_res = parse_dict_field(data, "storage_name", NonEmptyStr.parse)

    def validate_write_mode() -> Result[WriteMode, ValueErr]:
        if "write_mode" not in data:
            return Result.Ok(WriteMode.REPLACE)
        return parse_dict_field(data, "write_mode", WriteMode.parse)

    def validate_data_field_name() -> Result[NonEmptyStr | None, ValueErr]:
        if "data_field_name" not in data:
            return Result.Ok(None)
        return parse_dict_field(data, "data_field_name", NonEmptyStr.parse)

    def validate_return_empty_result() -> Result[bool, ValueErr]:
        if "return_empty_result" not in data:
            return Result.Ok(False)
        return parse_dict_field(data, "return_empty_result", parse_bool_str)

    write_mode_res = validate_write_mode()
    data_field_name_res = validate_data_field_name()
    return_empty_result_res = validate_return_empty_result()

    return apply4(
        lambda sn, wm, dfn, rer: (sn, wm, dfn, rer),
        lambda errs: errs,
        storage_name_res,
        write_mode_res,
        data_field_name_res,
        return_empty_result_res,
    )


def _parse_config_data(data: dict[str, Any]) -> Result[SaveDataConfigData, tuple[ValueErr, ...]]:
    """Parse SaveDataConfigData variant. id_field_name is required."""
    common_res = _parse_common_fields(data)
    id_field_name_res = parse_dict_field(data, "id_field_name", NonEmptyStr.parse)

    return apply(
        lambda common, idf: SaveDataConfigData(
            storage_name=common[0],
            id_field_name=idf,
            write_mode=common[1],
            data_field_name=common[2],
            return_empty_result=common[3],
        ),
        lambda errs: errs,
        common_res,
        id_field_name_res,
    )


def _parse_config_metadata(data: dict[str, Any]) -> Result[SaveDataConfigMetadata, tuple[ValueErr, ...]]:
    """Parse SaveDataConfigMetadata variant. id_field_name is required."""
    common_res = _parse_common_fields(data)
    id_field_name_res = parse_dict_field(data, "id_field_name", NonEmptyStr.parse)

    return apply(
        lambda common, idf: SaveDataConfigMetadata(
            storage_name=common[0],
            id_field_name=idf,
            write_mode=common[1],
            data_field_name=common[2],
            return_empty_result=common[3],
        ),
        lambda errs: errs,
        common_res,
        id_field_name_res,
    )


def _parse_config_static(data: dict[str, Any]) -> Result[SaveDataConfigStatic, tuple[ValueErr, ...]]:
    """Parse SaveDataConfigStatic variant. fixed_id is required, no id_field_name."""
    common_res = _parse_common_fields(data)
    fixed_id_res = parse_dict_field(data, "fixed_id", NonEmptyStr.parse)

    return apply(
        lambda common, fid: SaveDataConfigStatic(
            storage_name=common[0],
            fixed_id=fid,
            write_mode=common[1],
            data_field_name=common[2],
            return_empty_result=common[3],
        ),
        lambda errs: errs,
        common_res,
        fixed_id_res,
    )


def parse_save_data_config(data: dict[str, Any]) -> Result[SaveDataConfig, tuple[ValueErr, ...]]:
    """
    Top-level parser that dispatches to variant-specific parsers
    based on the 'id_source' discriminator field.
    Defaults to DATA if 'id_source' is absent.
    Returns typed errors.
    """
    def validate_id_source() -> Result[IdSource, ValueErr]:
        if "id_source" not in data:
            return Result.Ok(IdSource.DATA)
        return parse_dict_field(data, "id_source", IdSource.parse)

    def dispatch(id_source: IdSource) -> Result[SaveDataConfig, tuple[ValueErr, ...]]:
        match id_source:
            case IdSource.DATA:
                return _parse_config_data(data)
            case IdSource.METADATA:
                return _parse_config_metadata(data)
            case IdSource.CONFIG:
                return _parse_config_static(data)
            case _:
                return Result.Error((ValueInvalid("id_source", id_source),))

    return validate_id_source().map_error(lambda err: tuple([err]))\
        .bind(dispatch)
