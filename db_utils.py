import sqlite3
import pandas as pd


class DataframeToBase:
    def __init__(self, database_file):
        self.database_file = database_file
        self.connection = sqlite3.connect(database_file)
        self.cursor = self.connection.cursor()

    def create_table(self, dataframe, table_name):
        """Creates a table in the database if it doesn't exist.

        Args:
            dataframe: The pandas dataframe to be stored in the database.
            table_name: The name of the table to be created.
        """
        table_columns = list(dataframe.columns)
        create_table_statement = (
            f'CREATE TABLE IF NOT EXISTS {table_name} ({",".join(table_columns)})'
        )
        self.cursor.execute(create_table_statement)
        self.connection.commit()

    def insert_dataframe(self, dataframe, table_name, index=False):
        """Inserts a pandas dataframe into the database.

        Args:
            dataframe: The pandas dataframe to be inserted into the database.
            table_name: The name of the table to insert the dataframe into.
        """
        dataframe.to_sql(table_name, self.connection, index=index, if_exists="replace")
        self.connection.commit()

    def close(self):
        """Closes the connection to the database."""
        self.connection.close()
