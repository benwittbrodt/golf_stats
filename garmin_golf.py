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

    def db_conn(self, db):
        """
        Creates database connection with given database file
        """
        return sqlite3.connect(db)

    def get_id(self, conn):
        sql = f"select id from scorecard_summary"
        df = pd.read_sql_query(sql, conn)["id"].to_list()
        return df


class GarminToDB:
    """
    test
    """

    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = sqlite3.connect(self.db_file)

    def get_id(self, table):
        sql = f"select id from {table}"
        df = pd.read_sql_query(sql, self.conn)["id"].to_list()
        return df

    def filter(self):
        """
        t
        """
        # TODO need to get a filter for a newly processed dataframe using the id gathered
        # df1 = sc2[~sc2['id'].isin(get_id())]

    def db_append(self, df, table):
        return df.to_sql(table, self.conn, index=False, if_exists="append")
