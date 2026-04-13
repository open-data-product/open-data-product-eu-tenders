import os
import time
import xml.dom.minidom
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
    NOTICE_TYPE = "notice-type"
    PLACE_OF_PERFORMANCE = "place-of-performance"
    PROCEDURE_TYPE = "procedure-type"
    PUBLICATION_DATE = "publication-date"
    PUBLICATION_NUMBER = "publication-number"
    BUYER_NAME = "buyer-name"
    TITLE_PROC = "title-proc"
    ADDITIONAL_INFO_PROC = "additional-info-proc"
    DOCUMENT_URL_LOT = "document-url-lot"
    OPTION_DESCRIPTION_LOT = "option-description-lot"
    DESCRIPTION_LOT = "description-lot"
    DESCRIPTION_PART = "description-part"
    DESCRIPTION_PROC = "description-proc"
    CONTRACT_DURATION_START_DATE_LOT = "contract-duration-start-date-lot"
    CONTRACT_DURATION_START_DATE_PART = "contract-duration-start-date-part"
    CONTRACT_DURATION_END_DATE_LOT = "contract-duration-end-date-lot"
    CONTRACT_DURATION_END_DATE_PART = "contract-duration-end-date-part"
    WINNER_NAME = "winner-name"
    VEHICLE_TYPE_VAL_RES = "vehicle-type-val-res"
    MAIN_CLASSIFICATION_PROC = "main-classification-proc"
    DIRECT_AWARD_JUSTIFICATION_PROC = "direct-award-justification-proc"
    SELECTION_CRITERION_DESCRIPTION_LOT = "selection-criterion-description-lot"
    ORGANISATION_CONTACT_POINT_BUYER = "organisation-contact-point-buyer"
    ORGANISATION_TEL_BUYER = "organisation-tel-buyer"
    ORGANISATION_EMAIL_BUYER = "organisation-email-buyer"
    RENEWAL_MAXIMUM_LOT = "renewal-maximum-lot"
    TOTAL_VALUE = "total-value"
    AWARD_CRITERION_TYPE_LOT = "award-criterion-type-lot"

    def __init__(self, api_field):
        self.api_field = api_field


@TrackingDecorator.track_time
def search_ted_notices(
    results_details_path,
    results_file_path,
    query,
    fields: [Field],
    scope: Scope = "ACTIVE",
    clean=False,
    quiet=False,
):
    if not clean and os.path.exists(results_file_path):
        not quiet and print(f"✓ Already exists {os.path.basename(results_file_path)}")
        return

    notices = []

    api_fields = [f.api_field for f in fields]

    # Check how many notices exist
    ted_search_response = call_search_api(
        query, api_fields, scope, limit=1, page=1, quiet=quiet
    )
    total_notice_count = ted_search_response.totalNoticeCount

    page_size = 250
    page_index_start = 1
    page_index_end = total_notice_count // page_size + page_index_start + 1

    for page in tqdm(
        range(page_index_start, page_index_end), desc="Load notices", unit="page"
    ):
        ted_search_response = call_search_api(
            query, api_fields, scope, limit=page_size, page=page, quiet=quiet
        )
        notices.extend(ted_search_response.notices)

    # Keep only intended fields
    notices_dataframe = pd.DataFrame(notices)
    existing_cols = [c for c in api_fields if c in notices_dataframe.columns]
    notices_dataframe_filtered = notices_dataframe[existing_cols].copy()

    def _encode_linebreaks(val):
        if isinstance(val, str):
            return val.replace("\r", "\\r").replace("\n", "\\n")
        elif isinstance(val, dict):
            return {k: _encode_linebreaks(v) for k, v in val.items()}
        elif isinstance(val, list):
            return [_encode_linebreaks(v) for v in val]
        return val

    for col in existing_cols:
        notices_dataframe_filtered.loc[:, col] = notices_dataframe_filtered[col].apply(
            _encode_linebreaks
        )

    # Save results
    os.makedirs(os.path.join(os.path.dirname(results_file_path)), exist_ok=True)
    notices_dataframe_filtered.to_csv(
        results_file_path, index=False, encoding="utf-8-sig"
    )
    not quiet and print(f"✓ Save {os.path.basename(results_file_path)}")

    # Download details
    for notice in tqdm(notices, desc="Load notice details", unit="notice"):
        xml_url = notice["links"]["xml"]["MUL"]
        publication_number = notice["publication-number"]
        download_file(
            os.path.join(results_details_path, f"{publication_number}.xml"),
            xml_url,
            clean,
            quiet=True,
        )


def call_search_api(
    query, api_fields: [str], scope: Scope = "ACTIVE", limit=10, page=1, quiet=False
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
            "fields": api_fields,
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


def download_file(file_path, url, clean, quiet):
    if clean or not os.path.exists(file_path):
        try:
            data = requests.get(url)
            if str(data.status_code).startswith("2"):
                os.makedirs(os.path.join(os.path.dirname(file_path)), exist_ok=True)

                _, extension = os.path.splitext(file_path)
                if extension == ".xml":
                    xml_content = xml.dom.minidom.parseString(
                        data.content
                    ).toprettyxml()

                    # Remove empty lines
                    lines = xml_content.splitlines()
                    non_empty_lines = [line for line in lines if line.strip()]
                    xml_content = "\n".join(non_empty_lines)

                    with open(file_path, "w", encoding="utf-8") as file:
                        file.write(xml_content)
                    not quiet and print(f"✓ Download {os.path.basename(file_path)}")
                else:
                    with open(file_path, "wb", encoding="utf-8") as file:
                        file.write(data.content)
                    not quiet and print(f"✓ Download {os.path.basename(file_path)}")
            else:
                not quiet and print(f"✗️ Error: {str(data.status_code)}, url {url}")
        except Exception as e:
            print(f"✗️ Exception: {str(e)}, url {url}")

    else:
        not quiet and print(f"✓ Already exists {os.path.basename(file_path)}")
