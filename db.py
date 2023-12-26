import sqlite3
import pandas as pd


class GarminDB:
    """
    test
    """

    def __init__(self, db_file):
        self.db_file = db_file
        self.connection = None
        self.cursor = None
        self.id_lists = {}
        self.hole_hist_cols = {
            "number": "INTEGER",
            "strokes": "REAL",
            "penalties": "REAL",
            "handicapscore": "REAL",
            "putts": "REAL",
            "fairwayshotoutcome": "TEXT",
            "pinpositionlat": "REAL",
            "pinpositionlon": "REAL",
            "scorecardid": "INTEGER",
            "lastmodifieddt": "TEXT",
            "hole_length": "REAL",
            "hole_par": "INTEGER",
            "hole_handicap": "INTEGER",
            "hole_length_yards": "REAL",
        }
        self.course_cols = {"id": "INTEGER", "coursename": "TEXT"}
        self.club_type_cols = {
            "id": "INTEGER",
            "name": "TEXT",
            "shaftlength": "REAL",
            "loftangle": "REAL",
            "lieangle": "REAL",
            "valid": "INTEGER",
            "displayrange": "TEXT",
        }
        self.club_cols = {
            "id": "INTEGER",
            "clubtypeid": "INTEGER",
            "shaftlength": "REAL",
            "flextypeid": "TEXT",
            "averagedistance": "INTEGER",
            "advicedistance": "INTEGER",
            "retired": "INTEGER",
            "deleted": "INTEGER",
            "lastmodifiedtime": "TEXT",
            "name": "TEXT",
        }
        self.scorecard_cols = {
            "id": "INTEGER",
            "customerid": "TEXT",
            "playerprofileid": "INTEGER",
            "roundplayername": "TEXT",
            "connectdisplayname": "TEXT",
            "courseglobalid": "INTEGER",
            "coursesnapshotid": "INTEGER",
            "frontnineglobalcourseid": "INTEGER",
            "scoretype": "TEXT",
            "usehandicapscoring": "INTEGER",
            "usestrokecounting": "INTEGER",
            "distancewalked": "REAL",
            "stepstaken": "REAL",
            "starttime": "TEXT",
            "formattedstarttime": "TEXT",
            "endtime": "TEXT",
            "formattedendtime": "TEXT",
            "unitid": "TEXT",
            "roundtype": "TEXT",
            "inprogress": "INTEGER",
            "excludefromstats": "INTEGER",
            "holescompleted": "INTEGER",
            "publicround": "INTEGER",
            "score": "REAL",
            "playerhandicap": "REAL",
            "coursehandicapstr": "TEXT",
            "teebox": "TEXT",
            "handicaptype": "TEXT",
            "teeboxrating": "REAL",
            "teeboxslope": "INTEGER",
            "lastmodifieddt": "TEXT",
            "sensoronputter": "INTEGER",
            "handicappedstrokes": "REAL",
            "strokes": "INTEGER",
            "course_name": "TEXT",
            "front_par_strokes": "INTEGER",
            "back_par_strokes": "INTEGER",
            "total_par_strokes": "INTEGER",
            "total_course_length": "REAL",
            "total_course_length_yards": "REAL",
        }
        self.shot_cols = {
            "id": "INTEGER",
            "scorecardid": "INTEGER",
            "playerprofileid": "INTEGER",
            "shottime": "INTEGER",
            "shotorder": "INTEGER",
            "shottimezoneoffset": "INTEGER",
            "clubid": "INTEGER",
            "holenumber": "INTEGER",
            "autoshottype": "TEXT",
            "meters": "REAL",
            "shotsource": "TEXT",
            "shottype": "TEXT",
            "lastmodifiedtime": "TEXT",
            "startloc_lat": "REAL",
            "startloc_lon": "REAL",
            "startloc_lie": "TEXT",
            "startloc_liesource": "TEXT",
            "endloc_lat": "REAL",
            "endloc_lon": "REAL",
            "endloc_lie": "TEXT",
            "endloc_liesource": "TEXT",
            "shottime_format": "TEXT",
            "shottime_local": "TEXT",
            "yards": "REAL",
        }

    def connect(self):
        self.connection = sqlite3.connect(self.db_file)
        self.cursor = self.connection.cursor()

    def close(self):
        if self.connection:
            self.connection.close()

    def create_table(self, table_name, col_dict):
        self.connect()
        statement = f"CREATE TABLE IF NOT EXISTS {table_name} ({'id INTEGER PRIMARY KEY,' if 'id' not in col_dict.keys() else ''}{', '.join([f'{col} {col_dict[col]}' for col in col_dict.keys()])});"
        self.cursor.execute(statement)
        return self.connection.commit()

    def db_append(self, df, table):
        """
        Appends data from input dataframe to designated table
        """
        self.connect()
        return df.to_sql(table, self.connection, index=False, if_exists="append")

    def get_id(self, table):
        """
        Returns a list of ids, added to the id_lists for the given table. Used for appending data vs. overwriting
        """
        self.connect()
        sql = f"select id from {table}"
        self.id_lists[table] = pd.read_sql_query(sql, self.connection)["id"].to_list()
        return self.id_lists[table]

    def filter_df(self, new_data, table):
        """
        Returns dataframe with the new data that should be appended to db
        """
        return new_data[~new_data["id"].isin(self.id_lists[table])]
