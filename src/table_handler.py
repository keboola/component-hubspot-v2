import copy
import hashlib

from keboola.component.dao import TableDefinition
from keboola.csvwriter import ElasticDictWriter


class TableHandler:
    def __init__(self, table_definition: TableDefinition, writer: ElasticDictWriter):
        self.table_definition = table_definition
        self.writer = writer

    def shorten_column_names_in_table_definition(self):
        """
        Function to make the columns conform to KBC, max length of column is 64.
        """
        columns = self.writer.fieldnames

        key_map = {}
        for i, column_name in enumerate(columns):
            if len(column_name) >= 64:
                hashed = self._hash_column(column_name)
                key_map[column_name] = f"{column_name[:30]}_{hashed}"
                columns[i] = f"{column_name[:30]}_{hashed}"
        self.table_definition.columns = columns

        column_metadata = self.table_definition.table_metadata.column_metadata
        self.table_definition.table_metadata.column_metadata = {key_map.get(k, k): v for (k, v) in
                                                                column_metadata.items()}

    @staticmethod
    def _hash_column(columns_name: str) -> str:
        return hashlib.md5(columns_name.encode('utf-8')).hexdigest()

    def redefine_table_column_metadata(self, state_columns):
        """
        Only saves metadata of columns that are not defined in the state.
        Purpose: when downloading many objects with all properties, rewriting the column metadata every time wastes
        SAPI resources and elongates the job run, as each column metadata has to be reset. Once a column is saved in
        the state we know that the metadata for the column has been saved, so we should only write metadata for new
        columns.
        """

        new_metadata = {}
        for col_name in self.table_definition.table_metadata.column_metadata:
            if col_name not in state_columns:
                new_metadata[col_name] = self.table_definition.table_metadata.column_metadata[col_name]
        self.table_definition.table_metadata.column_metadata = new_metadata

    def writerows(self, row_dicts):
        self.writer.writerows(row_dicts)

    def writerow(self, row):
        self.writer.writerow(row)

    def close_writer(self):
        self.writer.close()

    @property
    def writer_fields(self):
        return copy.copy(self.writer.fieldnames)

    def swap_column_names_in_table_definition(self, column_swaps: dict):

        columns = self.writer.fieldnames

        key_map = {}
        for i, column_name in enumerate(columns):
            if column_name in list(column_swaps.keys()):
                key_map[column_name] = column_swaps[column_name]
                columns[i] = column_swaps[column_name]

        column_metadata = self.table_definition.table_metadata.column_metadata
        self.table_definition.table_metadata.column_metadata = {key_map.get(k, k): v
                                                                for (k, v) in
                                                                column_metadata.items()}
        primary_keys = self.table_definition.primary_key
        new_primary_keys = []
        for primary_key in primary_keys:
            if primary_key in list(column_swaps.keys()):
                new_primary_keys.append(column_swaps[primary_key])
            else:
                new_primary_keys.append(primary_key)
        self.table_definition.primary_key = new_primary_keys

        columns = self.table_definition.columns
        new_columns = []
        for column in columns:
            if column in list(column_swaps.keys()):
                new_columns.append(column_swaps[column])
            else:
                new_columns.append(column)
        self.table_definition.columns = new_columns
