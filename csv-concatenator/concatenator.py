#!/usr/bin/env python

import logging
import csv
import os
import pandas as pd

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
from pyclowder.datasets import get_file_list
from pyclowder.files import download, upload_to_dataset, delete


class CSVConcatenator(Extractor):
    """Create and upload image thumbnail and image preview."""

    def __init__(self):
        Extractor.__init__(self)

        # parse command line and load default logging configuration
        self.setup()

        self.columns_file = "column_mapping"
        self.merged_file = "concatenated"

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

    def load_standard_columns(self, cols_file):
        """Load a column mapping from a standard file"""
        standard_cols = {}
        with open(cols_file, 'r', encoding='utf-8-sig') as sc:
            reader = csv.reader(sc)
            headers = {}
            for row in reader:
                # For each row, map all columns to the most recent available occurring name
                if len(headers) == 0:
                    for i in range(len(row)):
                        headers[i] = row[i]
                    continue
                for col_idx in headers:
                    if not pd.isna(row[col_idx]) and len(row[col_idx]) > 0:
                        latest_name = row[col_idx]
                for col_idx in headers:
                    standard_cols[row[col_idx]] = latest_name

        return standard_cols

    def load_tabular_data(self, data_file):
        if data_file.endswith(".csv"):
            return pd.read_csv(data_file)
        elif data_file.endswith(".tsv"):
            return pd.read_csv(data_file, sep='\t')
        elif data_file.endswith(".xlsx"):
            return pd.read_excel(data_file)

    def check_message(self, connector, host, secret_key, resource, parameters):
        # Don't download file if there isn't at least one other CSV to concatenate
        #host = "http://host.docker.internal:8000" # TODO: Remove

        dataset_id = resource["parent"]["id"]
        file_ext = resource["file_ext"]

        if resource['name'] == self.merged_file + file_ext:
            connector.message_process(resource, "Filename matches concatenation output; ignoring file.")
            return CheckMessage.ignore

        # Check whether the dataset includes another CSV
        all_files = get_file_list(connector, host, secret_key, dataset_id)
        for f in all_files:
            fname = f['name']
            if fname.endswith(file_ext) and f['id'] != resource['id']:
                return CheckMessage.download
        connector.message_process(resource, "No concatenation targets found.")
        return CheckMessage.ignore

    def process_message(self, connector, host, secret_key, resource, parameters):
        # Process the file and upload the results
        #host = "http://host.docker.internal:8000"  # TODO: Remove

        inputfile = resource["local_paths"][0]
        dataset_id = resource["parent"]["id"]
        file_ext = resource["file_ext"]
        merged_output = self.merged_file + file_ext

        # Determine which CSV to append to and whether there are column mappings
        all_files = get_file_list(connector, host, secret_key, dataset_id)
        cols_id = None
        for f in all_files:
            fname = f['name']
            if fname == self.columns_file + file_ext:
                cols_id = f['id']
                break

        target_ids = []
        merge_exists = False
        for f in all_files:
            fname = f['name']
            if fname == merged_output:
                target_ids = [f['id']]
                merge_exists = True
                break
            elif fname.endswith(file_ext) and f['id'] != resource['id']:
                # If we don't find an existing merged file, we will merge all with this extension
                target_ids.append(f['id'])

        if cols_id is not None:
            connector.message_process(resource, "Loading " + self.columns_file + file_ext)
            targ = download(connector, host, secret_key, cols_id, ext=file_ext)
            standard_columns = self.load_standard_columns(targ)
        else:
            # Initialize the standard columns table
            standard_columns = {}

        merged = None
        if len(target_ids) > 0:
            # Load the just-uploaded file data
            new_data = self.load_tabular_data(inputfile)
            new_data.rename(columns=standard_columns, inplace=True)

            if merge_exists:
                # Download existing merged file and append new data to the end
                connector.message_process(resource, "Downloading merged file %s" % target_ids[0])
                targ = download(connector, host, secret_key, target_ids[0], ext=file_ext)
                source_data = self.load_tabular_data(targ)
                new_data = self.load_tabular_data(inputfile)

                connector.message_process(resource, "Appending new file")
                source_data.rename(columns=standard_columns, inplace=True)
                merged = pd.concat([source_data, new_data])
            else:
                # Iterate through all files with this extension and merge them
                column_set = 1
                column_sets = {}

                for targ_id in target_ids:
                    connector.message_process(resource, "Downloading file %s" % targ_id)
                    targ = download(connector, host, secret_key, targ_id, ext=file_ext)
                    source_data = self.load_tabular_data(targ)

                    if cols_id is None:
                        # Stash the column names for initializing columns file later
                        columns = sorted(source_data.columns)
                        exists = None
                        for i in column_sets:
                            if column_sets[i] == columns:
                                exists = i
                        if exists is None:
                            column_sets[column_set] = columns
                            column_set += 1
                    else:
                        # Perform renaming of existing data columns otherwise
                        source_data.rename(columns=standard_columns, inplace=True)

                    if merged is not None:
                        merged = pd.concat([merged, source_data])
                    else:
                        merged = source_data

                # Finally, merge the newly uploaded file
                merged = pd.concat([merged, new_data])

            if file_ext == ".tsv":
                merged.to_csv(merged_output, sep="\t", index=False)
            elif file_ext == ".xlsx":
                merged.to_excel(merged_output, index=False)
            else:
                merged.to_csv(merged_output, index=False)

            # Finally, upload the newly merged file
            file_id = upload_to_dataset(connector, host, secret_key, dataset_id, merged_output, check_duplicate=False)
            if merge_exists:
                # TODO: v2 can update existing concatenated file instead of doing this delete & replace
                if file_id != target_ids[0]:
                    connector.message_process(resource, "Deleting previous version of file: %s" % target_ids[0])
                    delete(connector, host, secret_key, target_ids[0])
            connector.message_process(resource, "Uploaded %s: %s" % (merged_output, file_id))

            # Delete local copies of files in the container
            os.remove(merged_output)


if __name__ == "__main__":
    extractor = CSVConcatenator()
    extractor.start()
