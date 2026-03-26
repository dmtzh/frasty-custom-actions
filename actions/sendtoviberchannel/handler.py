import asyncio
from dataclasses import dataclass
from typing import Any

import aiohttp
from expression import Result

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import Error
from shared.infrastructure.storage.repository import NotFoundError, StorageError
from shared.pipeline.actionhandler import DataDto
from shared.utils.asyncresult import async_ex_to_error_result, async_result, coroutine_result
from shared.utils.parse import parse_from_dict, parse_non_empty_str
from shared.utils.result import to_error_list

from customactionhandler import CustomActionHandler

from .config import ViberApiConfig
from .viberchannelsstore import ViberChannel, ViberChannelIdValue, viber_channels_storage

@dataclass(frozen=True)
class SendToViberChannelConfig:
    channel_id: ViberChannelIdValue
    title: str

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['SendToViberChannelConfig', str]:
        def validate_channel_id() -> Result[ViberChannelIdValue, str]:
            return parse_from_dict(data, "channel_id", ViberChannelIdValue.from_value_with_checksum)
        def validate_title() -> Result[str, str]:
            return parse_from_dict(data, "title", parse_non_empty_str)
        channel_id_res = validate_channel_id()
        title_res = validate_title()
        errs = to_error_list(channel_id_res, title_res)
        match errs:
            case []:
                return Result.Ok(SendToViberChannelConfig(channel_id_res.ok, title_res.ok))
            case _:
                return Result.Error(", ".join(errs))

type SendToViberChannelInput = list[DataDto]

_SEND_TO_VIBER_CHANNEL_KEY = "send_to_viber_channel"
@dataclass(frozen=True)
class ViberTextMessage:
    text: str
    @staticmethod
    def from_dict(title: str, message: dict):
        msg = ", ".join(f"{key} {value}" for key, value in message.items() if key != _SEND_TO_VIBER_CHANNEL_KEY)
        msg_with_title = f"{title} {msg}"
        return ViberTextMessage(msg_with_title)

class ViberChannelUnexpectedError(Error):
    '''Unexpected error in viber channel'''

@async_result
@async_ex_to_error_result(StorageError.from_exception)
async def _get_viber_channel(id: ViberChannelIdValue) -> Result[ViberChannel, NotFoundError]:
    opt_channel = await viber_channels_storage.get(id)
    match opt_channel:
        case None:
            return Result.Error(NotFoundError(f"Viber channel {id.to_value_with_checksum()} not found"))
        case channel:
            return Result.Ok(channel)

@async_result
@async_ex_to_error_result(ViberChannelUnexpectedError.from_exception)
async def _send_to_viber_channel(viber_api_config: ViberApiConfig, channel: ViberChannel, config: SendToViberChannelConfig, input: SendToViberChannelInput):
    @async_ex_to_error_result(ViberChannelUnexpectedError.from_exception)
    async def send_text_message(session: aiohttp.ClientSession, timeout: aiohttp.ClientTimeout, message: ViberTextMessage) -> Result[None, ViberChannelUnexpectedError]:
        request_json = {
            "text": message.text,
            "auth_token": channel.auth_token,
            "from": channel.from_,
            "type": "text"
        }
        async with session.request(method=viber_api_config.http_method, url=viber_api_config.url.value, json=request_json, timeout=timeout) as response:
            json = await response.json()
            match json["status"]:
                case 0:
                    return Result.Ok(None)
                case failed_status_num:
                    return Result.Error(ViberChannelUnexpectedError(f"Send failed with error {json["status_message"]} ({failed_status_num})"))
    def update_message_status(msg: dict, channel_id: ViberChannelIdValue, send_res: Result):
        status_msg = send_res.map(lambda _: "Success").default_with(str)
        msg.setdefault(_SEND_TO_VIBER_CHANNEL_KEY, {})[channel_id.to_value_with_checksum()] = status_msg
    
    tasks = []
    async with aiohttp.ClientSession() as session:
        timeout_15_seconds = aiohttp.ClientTimeout(total=15)
        for msg in input:
            viber_text_msg = ViberTextMessage.from_dict(config.title, msg)
            task = send_text_message(session, timeout_15_seconds, viber_text_msg)
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        for msg, send_res in zip(input, results):
            update_message_status(msg, config.channel_id, send_res)
        return input

@coroutine_result[NotFoundError | StorageError | ViberChannelUnexpectedError]()
async def _send_to_viber_channel_workflow(viber_api_config: ViberApiConfig, config: SendToViberChannelConfig, input: SendToViberChannelInput):
    viber_channel = await _get_viber_channel(config.channel_id)
    processed_messages = await _send_to_viber_channel(viber_api_config, viber_channel, config, input)
    return processed_messages

class SendToViberChannelHandler(CustomActionHandler[SendToViberChannelConfig, SendToViberChannelInput]):
    def __init__(self, viber_api_config: ViberApiConfig):
        self._viber_api_config = viber_api_config
    
    @property
    def action_name(self) -> ActionName:
        return ActionName("sendtoviberchannel")
    
    def validate_config(self, raw_config: dict[str, Any]) -> Result[SendToViberChannelConfig, Any]:
        return SendToViberChannelConfig.from_dict(raw_config)
    
    def validate_input(self, dto_list: list[DataDto]) -> Result[SendToViberChannelInput, Any]:
        if not dto_list:
            return Result.Error("input data is missing")
        return Result.Ok(dto_list)
    
    async def handle(self, config: SendToViberChannelConfig, input: SendToViberChannelInput) -> CompletedResult:
        def ok_to_completed_result(result_data: list[DataDto]):
            return CompletedWith.Data(result_data) if result_data else CompletedWith.NoData()
        def err_to_completed_result(err):
            err_msg = f"Failed to send messsages to viber channel {config.channel_id.to_value_with_checksum()}: {err}"
            return CompletedWith.Error(err_msg)
        
        send_to_viber_channel_res = await _send_to_viber_channel_workflow(self._viber_api_config, config, input)
        completed_result = send_to_viber_channel_res.map(ok_to_completed_result).default_with(err_to_completed_result)
        return completed_result