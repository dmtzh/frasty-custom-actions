import os

from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.filewithversionlimited import FileWithVersionLimited
from shared.infrastructure.storage.repositoryitemaction import ItemActionInAsyncRepositoryWithVersion
from shared.pipeline.actionhandler import DataDto

import config

class PreviousDataStore:
    def __init__(self):
        folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "FilterNewDataStorage")
        file_repo_with_ver = FileWithVersionLimited[str, list[DataDto], list[DataDto]](
            "PreviousData",
            lambda data: data,
            lambda data: data,
            JsonSerializer[list[DataDto]](),
            "json",
            folder_path,
            5
        )
        self._file_repo_with_ver = file_repo_with_ver
        self._item_action = ItemActionInAsyncRepositoryWithVersion(file_repo_with_ver)
    
    async def get(self, set_name: str):
        opt_ver_with_value = await self._file_repo_with_ver.get(set_name)
        match opt_ver_with_value:
            case (_, value):
                return value
            case None:
                return None
    
    async def append(self, set_name: str, data: list[DataDto]):
        match data:
            case []:
                return None
            case [*append_list]:
                def append_data(data: list[DataDto] | None):
                    return None, (data or []) + append_list
                return await self._item_action(append_data)(set_name)
            case _:
                raise ValueError(f"Invalid data {data}")

previous_data_storage = PreviousDataStore()