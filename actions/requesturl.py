import asyncio
from collections.abc import Coroutine
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Optional
from urllib.parse import urlparse

import aiohttp
from expression import Result, effect

from shared.action import Action, ActionName, ActionType
from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import Error
from shared.pipeline.actionhandler import DataDto
from shared.utils.asyncresult import async_ex_to_error_result
from shared.utils.parse import parse_from_dict, parse_value
from shared.utils.result import to_error_list, to_ok_list
from shared.utils.string import strip_and_lowercase

from customactionhandler import CustomActionHandlerWithoutConfig

REQUEST_URL_ACTION = Action(ActionName("requesturl"), ActionType.CUSTOM)

class Url:
    """
    A class representing a URL.

    This class cannot be instantiated directly. Instead,
    use the `parse` static method to create an instance.

    Attributes:
        _url (str): The URL string.

    Methods:
        parse(url: str) -> Url | None: Creates a new Url instance from
        a string, or returns None if the string is None or empty.
    """

    def __init__(self, url: str):
        """
        Private constructor. Do not use directly.

        Args:
            url (str): The URL string.
        """
        if not isinstance(url, str):
            raise TypeError("URL must be a string")
        self._url = url

    @property
    def value(self):
        return self._url

    @staticmethod
    def parse(url: str) -> Optional['Url']:
        """
        Parses a string into a Url instance if it is a valid URL.

        This method checks if the given URL string has a valid scheme 
        and network location, and if the scheme is either 'http' or 'https'. 
        If these conditions are met, it returns a new Url instance; otherwise, 
        it returns None.

        Args:
            url (str): The URL string to parse.

        Returns:
            Url | None: A Url instance if the string is a valid URL, 
                        or None if the string is None, empty, or invalid.
        """
        if url is None:
            return None
        match url.strip():
            case "":
                return None
            case url_stripped:
                result = urlparse(url_stripped)
                # If the URL does not have a scheme (e.g., http, https)
                # or a network location (e.g., www.example.com),
                # or if the scheme is not http or https, it's not a valid URL
                has_scheme_and_netloc = all([result.scheme, result.netloc])
                has_http_or_https_scheme = result.scheme in ["http", "https"]
                if has_scheme_and_netloc and has_http_or_https_scheme:
                    return Url(url_stripped)
                else:
                    return None

class HttpMethod(StrEnum):
    GET = "GET"
    POST = "POST"

    @staticmethod
    def parse(http_method: str) -> Optional['HttpMethod']:
        if http_method is None:
            return None
        match strip_and_lowercase(http_method):
            case "get":
                return HttpMethod.GET
            case "post":
                return HttpMethod.POST
            case _:
                return None

@dataclass(frozen=True)
class RequestUrlInput:
    url: Url
    http_method: HttpMethod
    headers: dict[str, str] | None
    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['RequestUrlInput', str]:
        def validate_headers() -> Result[dict[str, str] | None, str]:
            @effect.result[dict[str, str] | None, str]()
            def parse_headers():
                raw_headers_dict = yield from parse_from_dict(data, "headers", lambda headers: headers if isinstance(headers, dict) else None)
                if not raw_headers_dict:
                    return None
                all_keys_and_vals_str = all(isinstance(k, str) and isinstance(v, str) for k, v in raw_headers_dict.items())
                headers_dict: dict[str, str] = yield from parse_value(raw_headers_dict, "headers", lambda headers: headers if all_keys_and_vals_str else None)
                return headers_dict
            return parse_headers() if "headers" in data else Result.Ok(None)
        
        url_res = parse_from_dict(data, "url", Url.parse)
        http_method_res = parse_from_dict(data, "http_method", HttpMethod.parse)
        headers_res = validate_headers()
        errs = to_error_list(url_res, http_method_res, headers_res)
        match errs:
            case []:
                return Result.Ok(RequestUrlInput(url_res.ok, http_method_res.ok, headers_res.ok))
            case errs:
                return Result.Error(", ".join(errs))

class RequestUrlUnexpectedError(Error):
    '''Unexpected error when request url'''

class RequestUrlHandler(CustomActionHandlerWithoutConfig[list[RequestUrlInput]]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("requesturl")
    
    def validate_input(self, dto_list: list[DataDto]) -> Result[list[RequestUrlInput], Any]:
        if not dto_list:
            return Result.Error("input data is missing")
        data_res_list = [RequestUrlInput.from_dict(data) for data in dto_list]
        data_list = to_ok_list(*data_res_list)
        match data_list:
            case []:
                errs = to_error_list(*data_res_list)
                return Result.Error(", ".join(errs))
            case _:
                return Result.Ok(data_list)
    
    async def handle(self, input_list: list[RequestUrlInput]) -> CompletedResult:
        @async_ex_to_error_result(RequestUrlUnexpectedError.from_exception)
        async def request_data(session: aiohttp.ClientSession, timeout: aiohttp.ClientTimeout, input: RequestUrlInput) -> Result[dict[str, Any], RequestUrlUnexpectedError]:
            try:
                async with session.request(method=input.http_method, url=input.url.value, headers=input.headers, timeout=timeout) as response:
                    # bytes = await response.read()
                    # json = await response.json()
                    # content_stream = response.content
                    content = await response.text()
                    response_data_dict = {
                        "status_code": response.status,
                        "content_type": response.content_type,
                        "content": content
                    }
                    response_data_dict = {
                        "req. headers": dict(response.request_info.headers),
                        "resp. headers": dict(response.headers)
                    } | response_data_dict
                    return Result.Ok(response_data_dict)
            except asyncio.TimeoutError:
                return Result.Error(RequestUrlUnexpectedError(f"Request timeout {timeout.total} seconds"))
            except aiohttp.client_exceptions.ClientConnectorError:
                return Result.Error(RequestUrlUnexpectedError(f"Cannot connect to {input.url} ({input.http_method})"))
        
        tasks: list[Coroutine[Any, Any, Result[dict[str, Any], RequestUrlUnexpectedError]]] = []
        async with aiohttp.ClientSession() as session:
            timeout_15_seconds = aiohttp.ClientTimeout(total=15)
            for input in input_list:
                task = request_data(session, timeout_15_seconds, input)
                tasks.append(task)
            results = await asyncio.gather(*tasks)
            success_results = to_ok_list(*results)
            match success_results:
                case []:
                    errs = map(str, to_error_list(*results))
                    return CompletedWith.Error(", ".join(errs))
                case _:
                    return CompletedWith.Data(success_results)
