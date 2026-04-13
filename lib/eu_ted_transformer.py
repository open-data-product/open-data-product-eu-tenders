import ast
import os

import pandas as pd
from opendataproduct.tracking_decorator import TrackingDecorator


@TrackingDecorator.track_time
def transform_eu_tenders(
    source_path,
    results_path,
    fields,
    quiet=False,
):
    def _dedup_list_string(val):
        if pd.api.types.is_scalar(val) and pd.isna(val):
            return val
        try:
            parsed_list = ast.literal_eval(str(val))
            if isinstance(parsed_list, list):
                return str(list(dict.fromkeys(parsed_list)))
        except (ValueError, SyntaxError):
            pass
        return val

    def _extract_deu_or_eng(val):
        if pd.api.types.is_scalar(val) and pd.isna(val):
            return ""
        try:
            parsed_dict = ast.literal_eval(str(val))
            if isinstance(parsed_dict, dict):
                if "deu" in parsed_dict:
                    return parsed_dict["deu"]
                elif "eng" in parsed_dict:
                    return parsed_dict["eng"]
        except (ValueError, SyntaxError):
            pass
        return val

    def _unpack_single_list(val):
        if pd.api.types.is_scalar(val) and pd.isna(val):
            return val
        try:
            parsed = ast.literal_eval(str(val))
            if isinstance(parsed, list) and len(parsed) == 1:
                return parsed[0]
        except (ValueError, SyntaxError):
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

                # De-duplicate lists
                for col in fields:
                    if col in dataframe.columns:
                        dataframe[col] = dataframe[col].apply(_dedup_list_string)

                # Select preferred language
                for col in fields:
                    if col in dataframe.columns:
                        dataframe[col] = dataframe[col].apply(_extract_deu_or_eng)

                # Unpack single-item lists
                for col in fields:
                    if col in dataframe.columns:
                        dataframe[col] = dataframe[col].apply(_unpack_single_list)

                # Add notice URL
                if "publication-number" in dataframe.columns:
                    dataframe["notice-url"] = dataframe["publication-number"].apply(
                        lambda x: f"https://ted.europa.eu/de/notice/-/detail/{x}" if pd.api.types.is_scalar(x) and pd.notna(x) and str(x).strip() else pd.NA
                    )

                # Format organisation-tel-buyer
                if "organisation-tel-buyer" in dataframe.columns:
                    dataframe["organisation-tel-buyer"] = dataframe["organisation-tel-buyer"].apply(
                        lambda x: f"+{str(x).strip()}" if pd.notna(x) and str(x).strip().startswith("49") else x
                    )

                # Write results file
                os.makedirs(
                    os.path.join(os.path.dirname(results_file_path)), exist_ok=True
                )
                dataframe.to_csv(results_file_path, index=False, encoding="utf-8-sig")
                not quiet and print(f"✓ Transform {os.path.basename(results_file_path)}")
