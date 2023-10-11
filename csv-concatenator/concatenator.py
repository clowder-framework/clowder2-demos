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
        if resource['name'] == self.columns_file + file_ext:
            connector.message_process(resource, "Filename matches columns lookup; ignoring file.")
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
        columns_output = self.columns_file + file_ext

        # Determine which CSV to append to and whether there are column mappings
        all_files = get_file_list(connector, host, secret_key, dataset_id)
        cols_id = None
        for f in all_files:
            fname = f['name']
            if fname == columns_output:
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
            elif fname.endswith(file_ext) and f['id'] != resource['id'] and fname != columns_output:
                # If we don't find an existing merged file, we will merge all with this extension
                target_ids.append(f['id'])

        if cols_id is not None:
            connector.message_process(resource, "Loading " + columns_output)
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
                source_data.rename(columns=standard_columns, inplace=True)
                # Combine any same-named columns after the rename
                source_data = source_data.groupby(source_data.columns, axis=1).sum()

                connector.message_process(resource, "Appending new file")
                merged = pd.concat([source_data, new_data])
            else:
                # Iterate through all files with this extension and merge them
                column_set = 1
                column_sets = {}

                for targ_id in target_ids:
                    connector.message_process(resource, "Downloading file %s" % targ_id)
                    targ = download(connector, host, secret_key, targ_id, ext=file_ext)
                    source_data = self.load_tabular_data(targ)
                    source_data.rename(columns=standard_columns, inplace=True)

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

                    if merged is not None:
                        merged = pd.concat([merged, source_data])
                    else:
                        merged = source_data

                # Store newly uploaded file columns last, so they get choice preference
                if cols_id is None:
                    # Stash the column names for initializing columns file later
                    columns = sorted(new_data.columns)
                    exists = None
                    for i in column_sets:
                        if column_sets[i] == columns:
                            exists = i
                    if exists is None:
                        column_sets[column_set] = columns
                        column_set += 1

                # Finally, merge the newly uploaded file
                merged = pd.concat([merged, new_data])

            if file_ext == ".tsv":
                merged.to_csv(merged_output, sep="\t", index=False)
            elif file_ext == ".xlsx":
                merged.to_excel(merged_output, index=False)
            else:
                merged.to_csv(merged_output, index=False)

            if cols_id is None:
                # Restructure data for building CSV
                unique_cols = []
                col_csv_rows = []
                for i in column_sets:
                    for col_name in column_sets[i]:
                        if col_name not in unique_cols:
                            curr_row = {i: col_name}
                            for j in column_sets:
                                if i != j:
                                    curr_row[j] = col_name if col_name in column_sets[j] else ""
                            unique_cols.append(col_name)
                            col_csv_rows.append(curr_row)

                # Initialize the columns file for future runs
                with open(columns_output, 'w') as out:
                    # Header
                    col_strs = [str(x) for x in list(column_sets.keys())]
                    out.write(",".join(col_strs)+'\n')
                    for r in col_csv_rows:
                        row_vals = []
                        for i in column_sets:
                            row_vals.append(r[i])
                        out.write(",".join(row_vals)+'\n')

                # Upload the columns file
                upload_to_dataset(connector, host, secret_key, dataset_id, columns_output, check_duplicate=False)
                os.remove(columns_output)

            # Finally, upload the newly merged file
            file_id = upload_to_dataset(connector, host, secret_key, dataset_id, merged_output, check_duplicate=False)
            os.remove(merged_output)
            if merge_exists:
                # TODO: v2 can update existing concatenated file instead of doing this delete & replace
                if file_id != target_ids[0]:
                    connector.message_process(resource, "Deleting previous version of file: %s" % target_ids[0])
                    delete(connector, host, secret_key, target_ids[0])
            connector.message_process(resource, "Uploaded %s: %s" % (merged_output, file_id))


if __name__ == "__main__":
    extractor = CSVConcatenator()
    extractor.start()
