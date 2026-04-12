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
        if pd.isna(val):
            return val
        try:
            parsed_list = ast.literal_eval(str(val))
            if isinstance(parsed_list, list):
                return str(list(dict.fromkeys(parsed_list)))
        except (ValueError, SyntaxError):
            pass
        return val

    def _extract_deu_or_eng(val):
        if pd.isna(val):
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

                # Write results file
                os.makedirs(
                    os.path.join(os.path.dirname(results_file_path)), exist_ok=True
                )
                dataframe.to_csv(results_file_path, index=False, encoding="utf-8-sig")
                not quiet and print(f"✓ Tranform {os.path.basename(results_file_path)}")
