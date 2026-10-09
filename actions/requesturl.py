import asyncio
from collections.abc import Coroutine
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Optional
from urllib.parse import urlparse

import aiohttp
from expression import Result, effect

from shared.action import ActionName
from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import Error
from shared.pipeline.actionhandler import DataDto
from shared.utils.parse import PositiveInt, parse_dict_field, parse_val
from shared.utils.result import apply4, to_ok_list, sequence_accumulating, traverse_accumulating_with_index
from shared.utils.string import strip_and_lowercase
from shared.validation import format_value_errors, ValueError as ValueErr, ValueMissing, with_prefix

from customactionhandler import CustomActionHandler

# Default timeout for a single HTTP request, in seconds.
_DEFAULT_REQUEST_TIMEOUT_SECONDS: int = 15

@dataclass(frozen=True)
class RequestUrlConfig:
    delay_between_requests: int

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result['RequestUrlConfig', ValueErr]:
        if "delay_between_requests" not in data:
            delay_res: Result[int, ValueErr] = Result.Ok(0)
        else:
            delay_res = parse_dict_field(data, "delay_between_requests", PositiveInt.parse)

        return delay_res.map(RequestUrlConfig)

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

    def __str__(self):
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
    json: dict[str, Any] | None
    data: DataDto

    @staticmethod
    def from_dict(data: DataDto) -> Result['RequestUrlInput', tuple[ValueErr, ...]]:
        @effect.result[dict[str, str] | None, ValueErr]()
        def validate_headers():
            # Optional key: yield Ok(None) explicitly. A bare `return None` inside an
            # @effect.result generator hits the builder's `zero` case, which raises
            # NotImplementedError in expression 5.x instead of returning Ok(None).
            if "headers" not in data:
                return (yield from Result.Ok(None))
            raw_headers = yield from parse_dict_field(data, "headers", lambda raw_headers: raw_headers if isinstance(raw_headers, dict) else None)
            valid_headers = yield from parse_val(raw_headers, "headers", lambda raw: raw if all(isinstance(k, str) and isinstance(v, str) for k, v in raw.items()) else None)
            return valid_headers

        @effect.result[dict[str, Any] | None, ValueErr]()
        def validate_json():
            # Optional key: yield Ok(None) explicitly. A bare `return None` inside an
            # @effect.result generator hits the builder's `zero` case, which raises
            # NotImplementedError in expression 5.x instead of returning Ok(None).
            if "json" not in data:
                return (yield from Result.Ok(None))
            # For all other cases (including an explicit None/null value) the existing
            # validation below is kept unchanged.
            raw_json = yield from parse_dict_field(data, "json", lambda raw_json: raw_json if isinstance(raw_json, dict) else None)
            valid_json = yield from parse_val(raw_json, "json", lambda raw: raw if all(isinstance(k, str) for k in raw.keys()) else None)
            return valid_json

        url_res = parse_dict_field(data, "url", Url.parse)
        http_method_res = parse_dict_field(data, "http_method", HttpMethod.parse)
        headers_res = validate_headers()
        json_res = validate_json()

        return apply4(
            lambda url, http_method, headers, json: RequestUrlInput(url, http_method, headers, json, data),
            lambda errs: errs,
            url_res, http_method_res, headers_res, json_res
        )

class RequestUrlUnexpectedError(Error):
    '''Unexpected error when request url'''

def _format_unexpected_errors(errors: tuple[RequestUrlUnexpectedError, ...]) -> str:
    """
    Format a tuple of request errors into a single human-readable string.

    Mirrors ``format_value_errors`` from the validation module: individual
    error messages are joined with a comma separator, so the pipeline core
    (CompletedWith.Error) receives a single string.
    """
    return ", ".join(err.message for err in errors)

class RequestUrlHandler(CustomActionHandler[RequestUrlConfig, list[RequestUrlInput]]):
    @property
    def action_name(self) -> ActionName:
        return ActionName("requesturl")

    def validate_config(self, raw_config: dict[str, Any]) -> Result[RequestUrlConfig, Any]:
        return RequestUrlConfig.from_dict(raw_config)\
            .map_error(lambda err: (err,))\
            .map_error(format_value_errors)

    def validate_input(self, _: RequestUrlConfig, dto_list: list[DataDto]) -> Result[list[RequestUrlInput], Any]:
        if not dto_list:
            err = ValueMissing("input_data")
            return Result.Error(format_value_errors((err,)))

        def validate_input_item(idx: int, data: DataDto) -> Result[RequestUrlInput, tuple[ValueErr, ...]]:
            return RequestUrlInput.from_dict(data)\
                .map_error(lambda errs: with_prefix(f"input_data[{idx}]", errs))

        input_res = traverse_accumulating_with_index(dto_list, validate_input_item)
        return input_res.map_error(format_value_errors)

    async def _request_data(
        self,
        session: aiohttp.ClientSession,
        delay_before_request: int,
        timeout: aiohttp.ClientTimeout,
        input_item: RequestUrlInput,
    ) -> Result[dict[str, Any], RequestUrlUnexpectedError]:
        """
        Execute a single HTTP request and merge the response into the input data.

        Returns Result.Ok with the merged response dictionary on success,
        or Result.Error with a domain-specific error on failure.
        """
        await asyncio.sleep(delay_before_request)
        try:
            async with session.request(
                method=input_item.http_method,
                url=input_item.url.value,
                headers=input_item.headers,
                json=input_item.json,
                timeout=timeout,
            ) as response:
                content = await response.text()
                response_data_dict = {
                    "status_code": response.status,
                    "content_type": response.content_type,
                    "content": content,
                }
                response_data_dict = (
                    input_item.data
                    | {"req. headers": dict(response.request_info.headers)}
                    | {"resp. headers": dict(response.headers)}
                    | response_data_dict
                )
                return Result.Ok(response_data_dict)
        except asyncio.TimeoutError:
            # Domain-specific: the request exceeded the configured timeout.
            return Result.Error(
                RequestUrlUnexpectedError(f"Request timeout {timeout.total} seconds")
            )
        except aiohttp.client_exceptions.ClientConnectorError:
            # Domain-specific: the target host is unreachable or refused the connection.
            return Result.Error(
                RequestUrlUnexpectedError(
                    f"Cannot connect to {input_item.url} ({input_item.http_method})"
                )
            )
        except Exception as ex:
            return Result.Error(RequestUrlUnexpectedError.from_exception(ex))

    def _build_request_tasks(
        self,
        session: aiohttp.ClientSession,
        timeout: aiohttp.ClientTimeout,
        input_list: list[RequestUrlInput],
        delay_between_requests: int,
    ) -> list[Coroutine[Any, Any, Result[dict[str, Any], RequestUrlUnexpectedError]]]:
        """
        Build a list of async request coroutines with staggered delays.

        Each subsequent task is scheduled with an additional delay of
        ``delay_between_requests`` seconds, so requests are spaced out evenly.
        """
        tasks: list[Coroutine[Any, Any, Result[dict[str, Any], RequestUrlUnexpectedError]]] = []
        delay_before_request = 0
        for input_item in input_list:
            task = self._request_data(session, delay_before_request, timeout, input_item)
            tasks.append(task)
            delay_before_request += delay_between_requests
        return tasks

    def _aggregate_responses(
        self,
        responses_res: list[Result[dict[str, Any], RequestUrlUnexpectedError]],
    ) -> Result[list[dict[str, Any]], tuple[RequestUrlUnexpectedError, ...]]:
        """
        Aggregate individual response results using best-effort semantics.

        If at least one request succeeded, return only the successful responses.
        If all requests failed, aggregate all errors with element index prefixes.
        """
        success_responses = to_ok_list(*responses_res)
        match success_responses:
            case []:
                # All requests failed: collect every error with its position.
                responses_res_iter = (
                    response_res.map_error(
                        lambda err: RequestUrlUnexpectedError(f"[{idx}]: {err.message}")
                    )
                    for idx, response_res in enumerate(responses_res)
                )
                return sequence_accumulating(responses_res_iter)
            case _:
                # At least one request succeeded: return successful responses only.
                return Result.Ok(success_responses)

    async def handle(
        self, config: RequestUrlConfig, input_list: list[RequestUrlInput]
    ) -> CompletedResult:
        # --- Step 1: Prepare session configuration ---
        timeout = aiohttp.ClientTimeout(total=_DEFAULT_REQUEST_TIMEOUT_SECONDS)

        # --- Step 2: Execute parallel HTTP requests with staggered delays ---
        async with aiohttp.ClientSession() as session:
            tasks = self._build_request_tasks(
                session, timeout, input_list, config.delay_between_requests
            )
            responses_res = await asyncio.gather(*tasks)

        # --- Step 3: Aggregate responses using best-effort semantics ---
        aggregated_res = self._aggregate_responses(responses_res).map_error(
            _format_unexpected_errors
        )

        # --- Step 4: Convert the aggregated result into a CompletedResult ---
        def ok_to_completed_result(result_list: list[DataDto]) -> CompletedResult:
            return CompletedWith.Data(result_list)

        def err_to_completed_result(err: str) -> CompletedResult:
            return CompletedWith.Error(err)

        return aggregated_res.map(ok_to_completed_result).default_with(err_to_completed_result)
