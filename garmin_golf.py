import json
import pandas as pd
import os
import re
import fitdecode
import toml

config = toml.load("config.toml")
print(config)


class GarminData:
    """
    test
    """

    def __init__(self):
        self.folder_path = None
        self.fit_scorecard = None
        self.fit_scorecard_clean_df = None
        self.fit_hole_history = None
        self.fit_hole_clean_df = None
        self.scorecard_for_holes = None
        self.json_data = None
        self.source_path()
        self.load_raw_data()

    def source_path(self):
        current_directory = os.getcwd()
        golf_dir = "garmin_data/DI_CONNECT"
        di_golf = "DI-GOLF"
        self.folder_path = os.path.join(current_directory, golf_dir, di_golf)

    ########### POSSIBLY REMOVE ###########
    def open_file(self, filename):
        """
        Loads a dataset from the garmin folder based off of the base file name (without .json)
        Output should be a json object
        """
        files_avail = os.listdir(self.folder_path)

        if filename + ".json" in files_avail:
            with open(f"{os.path.join(self.folder_path,filename)}.json") as file:
                return json.load(file)
        else:
            print("File not found, make sure the filepath is valid")

    #######################################

    def load_raw_data(self):
        """
        Loads all json data into a dict for processing later in the class
        """
        all_files = os.listdir(self.folder_path)
        json_files = [file for file in all_files if file.endswith(".json")]
        json_data = {}

        for filename in json_files:
            with open(f"{os.path.join(self.folder_path,filename)}") as file:
                data = json.load(file)
                json_data[data["type"].lower()] = data
        self.json_data = json_data
        return json_data.keys()

    def convert_gps(self, column):
        """
        Converts garmin's semicircle GPS coordinate to deimals
        """
        return column * (180 / (2**31))

    def process_club_types(self):
        """
        Creates dataframe for DB import for Golf-CLUB_TYPES file
        Also can be used for the Golf-CLUB data file
        """
        json = self.json_data["club_types"]
        df = pd.DataFrame.from_dict(json["data"])
        df.rename({"value": "id"}, axis=1, inplace=True)
        df.columns = df.columns.str.lower()
        return df

    def proc_course(self):
        """
        Input: json data from the Golf-COURSE json file or open_file output
        Output: dataframe ready for the database (id, coursename)
        Requires needs_id = False in db function
        """
        json = self.json_data["course"]
        df = pd.DataFrame.from_dict(json)
        df["id"] = df.index.astype("int64")

        df = df.reset_index()
        df1 = df[["id", "data"]].copy()
        df1.rename(columns={"data": "coursename"}, inplace=True)
        return df1

    def proc_scorecard(self):
        """
        Loads scorecard data and transforms columns to lower case - returns a dataframe to be used in processing the hole history
        """
        json = self.json_data["scorecard"]
        df = pd.DataFrame.from_dict(json["data"])
        df.columns = df.columns.str.lower()
        self.scorecard_for_holes = df
        df.pop("holes")
        df2 = pd.merge(
            left=df,
            right=self.fit_scorecard_clean_df,
            left_on="id",
            right_on="scorecardid",
        )
        df2["total_course_length_yards"] = df2["total_course_length_yards"].round(2)
        df2.drop(["scorecardid"], axis=1, inplace=True)
        return df2

    def proc_hole_history(self):
        """
        Takes the scorecard DF and processes the hole performance for each hole on the scorecard
        Must be done before the scorecard is full processed (need the hole json data)
        """
        all_holes = pd.DataFrame()
        for i in range(0, len(self.scorecard_for_holes["holes"])):
            scorecard_id = self.scorecard_for_holes["id"][i]
            hole_df = pd.DataFrame.from_dict(
                self.scorecard_for_holes[["id", "holes"]]["holes"][i]
            )
            hole_df["scorecardid"] = scorecard_id
            all_holes = pd.concat([all_holes, hole_df])

        all_holes.columns = all_holes.columns.str.lower()
        output = all_holes.reset_index(drop=True)
        output["pinpositionlat"] = self.convert_gps(output["pinpositionlat"])
        output["pinpositionlon"] = self.convert_gps(output["pinpositionlon"])
        df2 = pd.merge(
            output,
            self.fit_hole_clean_df,
            left_on=["scorecardid", "number"],
            right_on=["scorecardid", "hole_number"],
        )
        df2.drop(["hole_number"], axis=1, inplace=True)
        return df2

    def proc_shots(self):
        """
        Processes Golf-SHOT.json data into a formatted dataframe.
        Converts the latitude and longitude into the decimal degrees, shot time to yyyy-mm-dd hh:mm:ss and also localizes it to when the shot was taken
        Converts meters to yards and adds new column, because 'MURICA
        """
        json = self.json_data["shot"]["data"]

        df = pd.json_normalize(json, sep="_")
        df.columns = df.columns.str.lower()
        df["startloc_lat"] = self.convert_gps(df["startloc_lat"])
        df["startloc_lon"] = self.convert_gps(df["startloc_lon"])
        df["endloc_lat"] = self.convert_gps(df["endloc_lat"])
        df["endloc_lon"] = self.convert_gps(df["endloc_lon"])
        df["shottime_format"] = pd.to_datetime(df["shottime"], unit="ms").astype("str")
        df["shottime_local"] = pd.to_datetime(
            (df["shottime"] + df["shottimezoneoffset"]), unit="ms"
        ).astype("str")
        df["yards"] = df["meters"] * 1.0936133
        return df

    def parse_fit(self):
        for_scorecard = []

        field_map_190 = {
            0: "courseglobalid",
            1: "course_name",
            8: "front_par_strokes",
            9: "back_par_strokes",
            10: "total_par_strokes",
            14: "total_course_length",
        }

        for_hole_history = []
        field_map_193 = {
            0: "hole_number",
            1: "hole_length",
            2: "hole_par",
            3: "hole_handicap",
        }
        # field_map_192 = {1: "hole_number", 2: "hole_score", 5: "putts"}

        folder = "/Users/ben/projects/golf_stats/garmin_data/DI_CONNECT/DI-GOLF/"
        # Define a regular expression pattern to match the number at the end of the filename
        pattern = re.compile(r"-(\d+)\.fit$")

        for filename in os.listdir(folder):
            # Use the pattern to search for a match in the filename
            match = pattern.search(filename)

            # Extract the 'id' if a match is found
            if match:
                id_number = match.group(1)

            if filename.endswith(".fit"):
                # print(os.path.join(folder, filename))

                with fitdecode.FitReader(os.path.join(folder, filename)) as fit:
                    for frame in fit:
                        if frame.frame_type == fitdecode.FIT_FRAME_DATA:
                            # name of 190 is the course info
                            if frame.name == "unknown_190":
                                scorecard_dict = {}
                                for i in [0, 1, 8, 9, 10, 14]:
                                    scorecard_dict[field_map_190[i]] = frame.get_value(
                                        i
                                    )

                                scorecard_dict["scorecardid"] = id_number
                                for_scorecard.append(scorecard_dict)

                            # name of 193 is the hole info for hole_history
                            if frame.name == "unknown_193":
                                row_dict = {}
                                for i in range(0, 4):
                                    row_dict[field_map_193[i]] = frame.get_value(i)
                                row_dict["scorecardid"] = id_number
                                for_hole_history.append(row_dict)

        self.fit_scorecard = for_scorecard
        self.fit_hole_history = for_hole_history

    def fit_hole_clean(self):
        df = pd.DataFrame.from_dict(self.fit_hole_history)
        df["hole_length"] = df["hole_length"] / 100
        df["hole_length_yards"] = (df["hole_length"] * 1.0936133).round(2)
        df["scorecardid"] = df["scorecardid"].astype("int64")
        self.fit_hole_clean_df = df
        return df

    def fit_scorecard_clean(self):
        df = pd.DataFrame.from_dict(self.fit_scorecard)
        df["scorecardid"] = df["scorecardid"].astype("Int64")
        df["total_course_length_yards"] = df["total_course_length"] * 1.0936133
        df.pop("courseglobalid")
        self.fit_scorecard_clean_df = df
        return df
