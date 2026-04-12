import os
import time
from enum import Enum
from typing import Optional

import pandas as pd
import requests
from opendataproduct.tracking_decorator import TrackingDecorator
from pydantic import BaseModel, Field
from tqdm import tqdm

BASE_URL = "https://api.ted.europa.eu"
SEARCH_API = f"{BASE_URL}/v3/notices/search"


class TedSearchResponse(BaseModel):
    notices: list[dict] = Field(default_factory=list)
    totalNoticeCount: int = 0
    iterationNextToken: Optional[str] = None
    timedOut: bool = False


class Scope(Enum):
    ALL = "ALL"
    ACTIVE = "ACTIVE"
    LATEST = "LATEST"


def build_query(search_term=None):
    """
    Builds query
    :param search_term: search term
    :return:
    """
    query = ""

    if search_term:
        # FT = Full-text
        query += f"FT~{search_term}"

    return query


class Field(Enum):
    PUBLICATION_NUMBER = "ND"


def build_fields(fields: [Field]):
    return [f.value for f in fields]


@TrackingDecorator.track_time
def search_ted_notices(
    results_file_path, query, fields, scope: Scope = "ACTIVE", quiet=False
):
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

    # Keep only intended fields
    notices_dataframe = pd.DataFrame(notices)
    existing_cols = [c for c in fields if c in notices_dataframe.columns]
    notices_dataframe_filtered = notices_dataframe[existing_cols]

    # Save results
    os.makedirs(os.path.join(os.path.dirname(results_file_path)), exist_ok=True)
    notices_dataframe_filtered.to_csv(results_file_path, index=False)
    not quiet and print(f"✓ Save {os.path.basename(results_file_path)}")


def call_search_api(
    query, fields, scope: Scope = "ACTIVE", limit=10, page=1, quiet=False
) -> TedSearchResponse:
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
