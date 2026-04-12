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

                # TODO

                # Write results file
                os.makedirs(
                    os.path.join(os.path.dirname(results_file_path)), exist_ok=True
                )
                dataframe.to_csv(results_file_path)
                not quiet and print(f"✓ Tranform {os.path.basename(results_file_path)}")
