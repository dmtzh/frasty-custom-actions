from collections.abc import Generator
from dataclasses import dataclass
import os
from typing import Any

from expression import Result, effect

from shared.customtypes import IdValue
from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.filewithversion import FileWithVersion
from shared.utils.parse import parse_non_empty_str

import config

type ItemType = ViberChannel
type DtoItemType = dict[str, str]

class ViberChannelIdValue(IdValue):
    '''Viber channel id'''

@dataclass(frozen=True)
class ViberChannel:
    auth_token: str
    from_: str

    @staticmethod
    def to_dict(data: "ViberChannel") -> dict[str, str]:
        return {
            "auth_token": data.auth_token,
            "from": data.from_
        }
    
    @staticmethod
    @effect.result["ViberChannel", str]()
    def from_dict(raw_data: dict[str, str]) -> Generator[Any, Any, "ViberChannel"]:
        dict_data = yield from Result.Ok(raw_data) if isinstance(raw_data, dict) else Result.Error(f"Invalid data type {type(raw_data)}")
        auth_token = yield from parse_non_empty_str(dict_data.get("auth_token"), "auth_token")
        from_ = yield from parse_non_empty_str(dict_data.get("from"), "from")
        return ViberChannel(auth_token, from_)

class ViberChannelsStore:
    def __init__(self):
        folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "SendToViberChannelStorage")
        file_repo_with_ver = FileWithVersion[ViberChannelIdValue, ItemType, DtoItemType](
            "ViberChannels",
            ViberChannel.to_dict,
            ViberChannel.from_dict,
            JsonSerializer[DtoItemType](),
            "json",
            folder_path
        )
        self._file_repo_with_ver = file_repo_with_ver
    
    async def get(self, id: ViberChannelIdValue):
        opt_item_with_ver = await self._file_repo_with_ver.get(id)
        match opt_item_with_ver:
            case _, item:
                return item
            case _:
                return None

viber_channels_storage = ViberChannelsStore()