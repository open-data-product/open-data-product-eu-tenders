import json
import os
import xml.etree.ElementTree as ET

import pandas as pd
from opendataproduct.tracking_decorator import TrackingDecorator

from eu_ted_api_client import Field


@TrackingDecorator.track_time
def transform_eu_tenders(
    source_path,
    results_path,
    fields: [Field],
    quiet=False,
):
    def _fallback_empty_fields(row, api_field, xpath, xpath_attribute):
        is_empty = not row.get(api_field) or pd.isna(row.get(api_field))
        has_publication_number = row.get("publication-number")

        # Check if cell is empty and a publication number exists
        if is_empty and has_publication_number:
            publication_number = str(row.get("publication-number")).strip()
            xml_file_path = os.path.join(
                source_path, "eu-tenders-xml", f"{publication_number}.xml"
            )

            # Check if xml file exists
            if not os.path.exists(xml_file_path):
                return ""

            try:
                # Parse xml file
                tree = ET.parse(xml_file_path)
                root = tree.getroot()

                # Strip namespaces to allow simple xpath expressions
                for elem in root.iter():
                    if "}" in elem.tag:
                        elem.tag = elem.tag.split("}", 1)[1]

                # Search for values using xpath
                if not xpath_attribute:
                    matches = root.findall(xpath)
                    return " ".join([m.text for m in matches if m.text])
                else:
                    return root.find(xpath).attrib.get("CODE")
            except Exception:
                pass

        return row.get(api_field)

    def _dedup_list_string(val):
        if pd.api.types.is_scalar(val) and pd.isna(val) or val == "":
            return val

        val_str = str(val).strip()
        if not (val_str.startswith("[") and val_str.endswith("]")):
            return val

        try:
            parsed_list = json.loads(val_str.replace("'", '"'))
            if isinstance(parsed_list, list):
                return str(list(dict.fromkeys(map(str, parsed_list))))
        except (json.JSONDecodeError, TypeError, ValueError):
            pass

        return val

    def _extract_deu_or_eng(val):
        if pd.isna(val) or val == "" or not isinstance(val, str):
            return val if pd.notna(val) else ""

        val_trimmed = val.strip()
        if not (val_trimmed.startswith("{") and val_trimmed.endswith("}")):
            return val

        try:
            parsed_dict = json.loads(val_trimmed.replace("'", '"'))

            if isinstance(parsed_dict, dict):
                return parsed_dict.get("deu") or parsed_dict.get("eng") or val

        except (json.JSONDecodeError, TypeError, ValueError):
            pass

        return val

    def _unpack_single_list(val):
        if pd.isna(val) or not isinstance(val, str):
            return val

        val_trimmed = val.strip()
        if not (val_trimmed.startswith("[") and val_trimmed.endswith("]")):
            return val

        try:
            parsed = json.loads(val_trimmed.replace("'", '"'))
            if isinstance(parsed, list) and len(parsed) == 1:
                return parsed[0]
        except:
            pass
        return val

    for subdir, dirs, files in sorted(os.walk(source_path)):
        for file_name in sorted(files):
            _, file_extension = os.path.splitext(file_name)
            if file_extension == ".csv":
                source_file_path = os.path.join(
                    source_path, subdir.split(os.sep)[-1], file_name
                )
                results_file_path = os.path.join(
                    results_path,
                    subdir.split(os.sep)[-1],
                    file_name,
                )

                # Read source file
                dataframe = pd.read_csv(source_file_path)

                # Fill empty fields with values from XML using xpath
                for field in fields:
                    if field.xpath:
                        dataframe[field.api_field] = dataframe.apply(
                            _fallback_empty_fields,
                            axis=1,
                            args=(field.api_field, field.xpath, field.xpath_attribute),
                        )

                # De-duplicate lists
                for col in [f.api_field for f in fields]:
                    if col in dataframe.columns:
                        dataframe[col] = dataframe[col].apply(_dedup_list_string)

                # Select preferred language
                for col in [f.api_field for f in fields]:
                    if col in dataframe.columns:
                        dataframe[col] = dataframe[col].apply(_extract_deu_or_eng)

                # Unpack single-item lists
                for col in [f.api_field for f in fields]:
                    if col in dataframe.columns:
                        dataframe[col] = dataframe[col].apply(_unpack_single_list)

                # Add notice URL
                if "publication-number" in dataframe.columns:
                    dataframe["notice-url"] = dataframe["publication-number"].apply(
                        lambda x: f"https://ted.europa.eu/de/notice/-/detail/{x}"
                        if pd.api.types.is_scalar(x) and pd.notna(x) and str(x).strip()
                        else pd.NA
                    )

                # Format organisation-tel-buyer
                if "organisation-tel-buyer" in dataframe.columns:
                    dataframe["organisation-tel-buyer"] = dataframe[
                        "organisation-tel-buyer"
                    ].apply(
                        lambda x: f"+{str(x).strip()}"
                        if pd.notna(x) and str(x).strip().startswith("49")
                        else x
                    )

                # Write results file
                os.makedirs(
                    os.path.join(os.path.dirname(results_file_path)), exist_ok=True
                )
                dataframe.to_csv(results_file_path, index=False, encoding="utf-8-sig")
                not quiet and print(
                    f"✓ Transform {os.path.basename(results_file_path)}"
                )
