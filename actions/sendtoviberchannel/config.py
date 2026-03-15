from dataclasses import dataclass
from typing import Optional

from ..requesturl import HttpMethod, Url

@dataclass(frozen=True)
class ViberApiConfig:
    url: Url
    http_method: HttpMethod

    @staticmethod
    def parse(url: str, http_method: str) -> Optional["ViberApiConfig"]:
        opt_url = Url.parse(url)
        opt_http_method = HttpMethod.parse(http_method)
        if opt_url is None or opt_http_method is None:
            return None
        return ViberApiConfig(opt_url, opt_http_method)