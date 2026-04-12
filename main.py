# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "click>=8.2.1",
#     "open-data-product-python-lib",
#     "pandas>=3.0.2",
#     "pydantic>=2.12.5",
#     "requests>=2.33.1",
#     "rich>=14.1.0",
#     "tqdm>=4.67.3",
# ]
#
# [tool.uv.sources]
# open-data-product-python-lib = { git = "https://github.com/open-data-product/open-data-product-python-lib.git" }
# ///

import os
import sys

import click
from opendataproduct.config.data_product_manifest_loader import (
    load_data_product_manifest,
)
from opendataproduct.config.dpds_loader import load_dpds
from opendataproduct.config.odps_loader import load_odps
from opendataproduct.document.data_product_canvas_generator import (
    generate_data_product_canvas,
)
from opendataproduct.document.data_product_manifest_updater import (
    update_data_product_manifest,
)
from opendataproduct.document.dpds_canvas_generator import generate_dpds_canvas
from opendataproduct.document.dpds_updater import update_dpds
from opendataproduct.document.jupyter_notebook_creator import (
    create_jupyter_notebook_for_csv,
)
from opendataproduct.document.odps_canvas_generator import generate_odps_canvas
from opendataproduct.document.odps_updater import update_odps

from lib.eu_ted_api_client import search_ted_notices

file_path = os.path.realpath(__file__)
script_path = os.path.dirname(file_path)


@click.command()
@click.option("--clean", "-c", default=False, is_flag=True, help="Regenerate results.")
@click.option("--quiet", "-q", default=False, is_flag=True, help="Do not log outputs.")
def main(clean, quiet):
    data_path = os.path.join(script_path, "data")
    bronze_path = os.path.join(data_path, "01-bronze")
    silver_path = os.path.join(data_path, "02-silver")
    gold_path = os.path.join(data_path, "03-gold")
    docs_path = os.path.join(script_path, "docs")

    data_product_manifest = load_data_product_manifest(config_path=script_path)
    odps = load_odps(config_path=script_path)
    dpds = load_dpds(config_path=script_path)

    #
    # Bronze: Integrate
    #

    # Iterate over NUTS-1 regions
    for nuts_region in [
        "DE1",
        "DE2",
        "DE3",
        "DE4",
        "DE5",
        "DE6",
        "DE7",
        "DE8",
        "DE9",
        "DEA",
        "DEB",
        "DEC",
        "DED",
        "DEE",
        "DEF",
        "DEG",
    ]:
        query = (
            f"classification-cpv IN (60100000 60112000 60130000 60140000 60170000 60172000 60000000) "
            f"AND main-classification-proc NOT IN (60400000 60410000 60420000 60160000 60423000 45422000 64121200 60200000 64100000 60440000 63712000) "
            f"AND (place-of-performance IN ({nuts_region})) "
            f"SORT BY publication-number DESC"
        )
        fields = [
            "notice-type",
            "place-of-performance",
            "procedure-type",
            "publication-date",
            "publication-number",
            "buyer-name",
            "title-proc",
            "additional-info-proc",
            "document-url-lot",
            "option-description-lot",
            "description-lot",
            "description-part",
            "description-proc",
            "contract-duration-start-date-lot",
            "contract-duration-start-date-part",
            "contract-duration-end-date-lot",
            "contract-duration-end-date-part",
            "winner-name",
            "vehicle-type-val-res",
            "main-classification-proc",
            "direct-award-justification-proc",
            "selection-criterion-description-lot",
            "organisation-contact-point-buyer",
            "organisation-tel-buyer",
            "organisation-email-buyer",
            "renewal-maximum-lot",
            "total-value",
            "award-criterion-type-lot",
        ]

        search_ted_notices(
            results_file_path=os.path.join(
                bronze_path,
                "eu-tenders-public-transport",
                f"eu-tenders-public-transport-{nuts_region}.csv",
            ),
            query=query,
            fields=fields,
        )

    #
    # Documentation
    #

    create_jupyter_notebook_for_csv(
        data_product_manifest=data_product_manifest,
        results_path=script_path,
        data_path=bronze_path,
        clean=True,
        quiet=quiet,
    )

    update_data_product_manifest(
        data_product_manifest=data_product_manifest,
        config_path=script_path,
        data_paths=[bronze_path],
        file_endings=(".csv"),
        git_lfs=True,
    )

    update_odps(
        data_product_manifest=data_product_manifest,
        odps=odps,
        config_path=script_path,
    )

    update_dpds(
        data_product_manifest=data_product_manifest,
        dpds=dpds,
        config_path=script_path,
    )

    generate_data_product_canvas(
        data_product_manifest=data_product_manifest,
        docs_path=docs_path,
    )

    generate_odps_canvas(
        odps=odps,
        docs_path=docs_path,
    )

    generate_dpds_canvas(
        dpds=dpds,
        docs_path=docs_path,
    )


if __name__ == "__main__":
    main(sys.argv[1:])
