from collections.abc import Callable, Coroutine
import os
from typing import Any, Concatenate, ParamSpec, TypeVar

from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.filewithversionlimited import FileWithVersionLimited
from shared.infrastructure.storage.repositoryitemaction import ItemActionInAsyncRepositoryWithVersion
from shared.pipeline.actionhandler import DataDto

import config

P = ParamSpec("P")
R = TypeVar("R")

class PreviousDataStore:
    def __init__(self):
        folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "FilterNewDataStorage")
        file_repo_with_ver = FileWithVersionLimited[str, dict[str, list[DataDto]], dict[str, list[DataDto]]](
            "PreviousData",
            lambda data: data,
            lambda data: data,
            JsonSerializer[dict[str, list[DataDto]]](),
            "json",
            folder_path,
            5
        )
        self._file_repo_with_ver = file_repo_with_ver
        self._item_action = ItemActionInAsyncRepositoryWithVersion(file_repo_with_ver)
    
    def with_storage(self, func: Callable[Concatenate[dict[str, list[DataDto]] | None, P], tuple[R, dict[str, list[DataDto]]]]):
        def wrapper(set_name: str, *args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, R]:
            return self._item_action(func)(set_name, *args, **kwargs)
        return wrapper

previous_data_storage = PreviousDataStore()