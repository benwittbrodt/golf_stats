import json
import sqlite3
import pandas as pd


class GarminData:
    """
    test
    """

    def __init__(self, dataset):
        with open(dataset, "r") as f:
            self.dataset = json.load(f)

    def scorecard_summary(self):
        """
        Creates a dataframe with the scorecard summaries from the json data supplied. Returns said dataframe. pk = id column
        """
        scorecard_sums = self.dataset["summary"]["scorecardSummaries"]

        scorecard_summaries = pd.DataFrame(scorecard_sums)
        scorecard_summaries.columns = scorecard_summaries.columns.str.lower()
        return scorecard_summaries


class GarminToDB:
    """
    test
    """

    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = sqlite3.connect(self.db_file)
        self.id_lists = {}

    def get_id(self, table):
        """
        Returns a list of ids, added to the id_lists for the given table. Used for appending data vs. overwriting
        """
        sql = f"select id from {table}"
        self.id_lists[table] = pd.read_sql_query(sql, self.conn)["id"].to_list()
        return self.id_lists[table]

    def filter_df(self, new_data, table):
        """
        Returns dataframe with the new data that should be appended to db
        """
        return new_data[~new_data["id"].isin(self.id_lists[table])]

    def db_append(self, df, table):
        """
        Appends data from input dataframe to designated table
        """
        return df.to_sql(table, self.conn, index=False, if_exists="append")
