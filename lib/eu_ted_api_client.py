import time
from enum import Enum
from typing import Optional

import requests
from pydantic import BaseModel, Field
from tqdm import tqdm

BASE_URL = "https://api.ted.europa.eu"
SEARCH_API = f"{BASE_URL}/v3/notices/search"


class Links(BaseModel):
    xml: dict[str, str] = Field(default_factory=dict)
    pdf: dict[str, str] = Field(default_factory=dict)
    pdfs: dict[str, str] = Field(default_factory=dict)
    html: dict[str, str] = Field(default_factory=dict)
    htmlDirect: dict[str, str] = Field(default_factory=dict)


class TedNotice(BaseModel):
    ND: Optional[str] = None
    links: Optional[Links] = None


class TedSearchResponse(BaseModel):
    notices: list[TedNotice] = Field(default_factory=list)
    totalNoticeCount: int = 0
    iterationNextToken: Optional[str] = None
    timedOut: bool = False


class Scope(Enum):
    ALL = "ALL"
    ACTIVE = "ACTIVE"
    LATEST = "LATEST"


def search_ted_notices(query, fields, scope: Scope = "ACTIVE", quiet=False):
    notices = []

    # Check how many notices exist
    ted_search_response = call_search_api(
        query, fields, scope, limit=1, page=1, quiet=quiet
    )
    total_notice_count = ted_search_response.totalNoticeCount

    page_size = 250
    page_index_start = 1
    page_index_end = total_notice_count // page_size + page_index_start + 1

    for page in tqdm(
        range(page_index_start, page_index_end), desc="Load page", unit="page"
    ):
        ted_search_response = call_search_api(
            query, fields, scope, limit=page_size, page=page, quiet=quiet
        )
        notices.extend(ted_search_response.notices)


def call_search_api(
    query, fields, scope: Scope = "ACTIVE", limit=10, page=1, quiet=False
):
    """
    Calls the EU TED Search API
    :param query: query
    :param fields: list of fields
    :param scope: scope
    :param limit: notices per page
    :param page: page index
    :param quiet:
    :return: TedSearchResponse
    """

    url = SEARCH_API
    response = safe_request(
        url=url,
        payload={
            "query": query,
            "fields": fields,
            "scope": scope if isinstance(scope, str) else scope.value,
            "limit": limit,
            "page": page,
        },
    )

    if not str(response.status_code).startswith("2"):
        not quiet and print(f"✗️ Error: {str(response.status_code)}, url {url}")

    return TedSearchResponse(**response.json())


def safe_request(url, payload, retries=5):
    """
    Calls an API including retries when it hits a rate limit
    :param url: URL
    :param payload: payload
    :param retries: number of retries
    """

    for attempt in range(retries):
        response = requests.post(url, json=payload)

        if response.status_code == 429:
            wait_time = int(response.headers.get("Retry-After", 2**attempt))
            print(f"Rate limited! Sleeping for {wait_time}s...")
            time.sleep(wait_time)
            continue

        return response
