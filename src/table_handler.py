import copy

from keboola.component.dao import TableDefinition
from keboola.csvwriter import ElasticDictWriter


class TableHandler:
    def __init__(self, table_definition: TableDefinition, writer: ElasticDictWriter):
        self.table_definition = table_definition
        self.writer = writer

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
