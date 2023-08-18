# from garmin import *
import json
from pprint import pprint
import pandas as pd
from db_utils import DataframeToBase

with open("golf-export.json") as data:
    golf_raw = json.load(data)

clubs = golf_raw["clubs"]
last10drive = golf_raw["last10DataDrive"]


def process_clubs(clubs):
    """
    Accumulates all of the club data in the summary of the garmin golf app
    """
    club_translate = {
        "clubTypeId": [1, 2, 3, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22, 23],
        "club_name": [
            "Driver",
            "3 Wood",
            "5 Wood",
            "3 Iron",
            "4 Iron",
            "5 Iron",
            "6 Iron",
            "7 Iron",
            "8 Iron",
            "9 Iron",
            "PW",
            "SW",
            "LW",
            "Putter",
        ],
    }
    # create translation dataframe for ids to club names
    translate = pd.DataFrame.from_dict(club_translate)
    clubs_df = pd.DataFrame.from_dict(clubs)
    # convert over the clubstats, which are nested for some reason
    club_stats = clubs_df["clubStats"]

    norm_stats = (
        pd.json_normalize(club_stats).dropna(how="all").drop(columns="lastModifiedTime")
    )
    norm_stats["id"] = norm_stats["id"].astype("int64")
    norm_stats["averageDistance"] = norm_stats["averageDistance"] * 1.049
    norm_stats["maxLifetimeDistance"] = norm_stats["maxLifetimeDistance"] * 1.049
    clubs_named = pd.merge(clubs_df, translate, on="clubTypeId")
    clubs_df = clubs_named.drop(columns="clubStats")
    df = pd.merge(clubs_df, norm_stats, on="id", suffixes=("_clubs", "_club_stats"))
    return df


def process_shots(shot_details):
    """
    Processes all shots in the dataset - these are individual shots from every round
    """
    df = pd.DataFrame()

    for hole in shot_details:
        hole_num = hole["holeNumber"]

        # pin = hole["pinPosition"]
        # we will still have some nesting under this
        try:
            shots_df = pd.DataFrame.from_dict(hole["shots"])
        except KeyError:
            continue
        # flatten the start/end location info
        start = pd.json_normalize(shots_df["startLoc"]).rename(
            lambda x: "startLoc." + x, axis="columns"
        )
        shots_df = pd.merge(shots_df, start, left_index=True, right_index=True)

        end = pd.json_normalize(shots_df["endLoc"]).rename(
            lambda x: "endLoc." + x, axis="columns"
        )
        shots_df = pd.merge(shots_df, end, left_index=True, right_index=True)

        shots_df.pop("startLoc")
        shots_df.pop("endLoc")
        # add some common data
        shots_df["holeNumber"] = hole_num

        # shots_df["pin"] = pin
        shots_df["timestamp"] = pd.to_datetime(shots_df["shotTime"], unit="ms")

        df = pd.concat([df, shots_df], ignore_index=True)

    df.columns = df.columns.str.lower()
    return df


# Drive dispersion
df1 = pd.DataFrame(last10drive.pop("shotDispersionDetails"))
df1.columns = df1.columns.str.lower()

df1["shottime"] = pd.to_datetime(df1["shottime"], format="%Y-%m-%d %H:%M:%S")
##


print(df1)


# df = process_clubs(clubs)

# conn = DataframeToBase("golf.db")
# conn.create_table(df, "club_summary")
# conn.insert_dataframe(df, "club_summary")
# conn.close()
