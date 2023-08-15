# from garmin import *
import json
from pprint import pprint
import pandas as pd

with open("golf-export.json") as data:
    golf_raw = json.load(data)

clubs = golf_raw["clubs"]


def process_clubs(clubs):
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
    return pd.merge(clubs_df, norm_stats, on="id", suffixes=("_clubs", "_club_stats"))


def process_shots(shot_details):
    df = pd.DataFrame()
    for hole in shot_details:
        hole_num = hole["holeNumber"]
        hole_img = hole["holeImageUrl"]
        # pin = hole["pinPosition"]
        # we will still have some nesting under this
        try:
            shots_df = pd.DataFrame.from_dict(hole["shots"])
        except:
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
        # add some common data
        shots_df["holeNumber"] = hole_num
        # shots_df["holeImageUrl"] = hole_img
        # shots_df["pin"] = pin
        df = pd.concat([df, shots_df], ignore_index=True)
    return df


print(process_shots(golf_raw["shotDetails"]))
