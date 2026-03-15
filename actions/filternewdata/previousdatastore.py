from collections.abc import Callable, Coroutine
import os
from typing import Any, Concatenate, ParamSpec, TypeVar

from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.filewithversionlimited import FileWithVersionLimited
from shared.infrastructure.storage.repositoryitemaction import ItemActionInAsyncRepositoryWithVersion
from shared.pipeline.actionhandler import DataDto

import config

type ItemType = dict[str, list[DataDto]]
type DtoItemType = dict[str, list[DataDto]]

def _id[T](item: T) -> T: return item

P = ParamSpec("P")
R = TypeVar("R")

class PreviousDataStore:
    def __init__(self):
        folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "FilterNewDataStorage")
        file_repo_with_ver = FileWithVersionLimited[str, ItemType, DtoItemType](
            "PreviousData",
            _id,
            _id,
            JsonSerializer[DtoItemType](),
            "json",
            folder_path,
            5
        )
        self._file_repo_with_ver = file_repo_with_ver
        self._item_action = ItemActionInAsyncRepositoryWithVersion(file_repo_with_ver)
    
    def with_storage(self, func: Callable[Concatenate[ItemType | None, P], tuple[R, ItemType]]):
        def wrapper(set_name: str, *args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, R]:
            return self._item_action(func)(set_name, *args, **kwargs)
        return wrapper

previous_data_storage = PreviousDataStore()