# from garmin import *
import json
from pprint import pprint
import pandas as pd
from db_utils import DataframeToBase

with open("golf-export.json") as data:
    golf_raw = json.load(data)

clubs = golf_raw["clubs"]
last10drive = golf_raw["last10DataDrive"]
last10approach = golf_raw["last10DataApproach"]
last10chip = golf_raw["last10DataChip"]
scorecard = golf_raw["details"]


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
def drive_dispersion(last10df):
    df1 = pd.DataFrame(last10df.pop("shotDispersionDetails"))
    df1.columns = df1.columns.str.lower()

    df1["shottime"] = pd.to_datetime(df1["shottime"], format="%Y-%m-%d %H:%M:%S")
    return df1


##


def process_last10_approach(last10):
    # app_insight = last10.pop("approachInsight")
    # dist_range_insights_df = pd.DataFrame(app_insight["distRangeInsights"])
    # club_insights_df = pd.DataFrame(app_insight["clubInsights"])
    shot_orientation_df = pd.DataFrame(last10.pop("shotOrientationDetail"))
    shot_orientation_df.columns = shot_orientation_df.columns.str.lower()
    return shot_orientation_df  # , shot_orientation_df, dist_range_insights_df, club_insights_df


# similar to drives and approach
def process_last10_chip(last10):
    shot_orientation_df = pd.DataFrame(last10.pop("shotOrientationDetail"))
    shot_orientation_df.columns = shot_orientation_df.columns.str.lower()
    return shot_orientation_df


def process_holes(scorecard, course_snap):
    # looks like: "544435434543444435", so the index is the hole # - 1
    pars = course_snap["holePars"]
    holes_df = pd.DataFrame(scorecard["holes"])
    # add id and course name, course id as scalars for convenience
    holes_df["scorecardId"] = scorecard["id"]
    holes_df["courseName"] = course_snap["name"]
    holes_df["courseGlobalId"] = course_snap["courseGlobalId"]
    holes_df["par"] = holes_df["number"].apply(lambda x: int(pars[x - 1]))
    holes_df["relative_to_par"] = holes_df["strokes"] - holes_df["par"]
    return holes_df


def process_scorecard(scorecards):
    cards_df = pd.DataFrame()
    courses_df = pd.DataFrame()
    all_holes_df = pd.DataFrame()
    for scorecard_container in scorecards:
        course_snap = scorecard_container["courseSnapshots"][0]
        course_snap_df = pd.DataFrame.from_dict(course_snap)
        # only one of these
        score_details = scorecard_container["scorecardDetails"][0]
        score_stats = score_details["scorecardStats"]  # only one of these
        name = course_snap_df["name"]
        the_scorecard = score_details["scorecard"]
        holes_df = process_holes(the_scorecard, course_snap)

        score_df = pd.DataFrame.from_dict(the_scorecard)
        score_df.drop(columns="holes")
        # TODO: convert types for dates
        for key, value in score_stats["round"].items():
            # TODO: is there a better pandas way?  Getting an error on index missing
            score_df[key] = value
        # Construct our data frames
        all_holes_df = pd.concat([all_holes_df, holes_df])
        cards_df = pd.concat([cards_df, score_df])
        courses_df = pd.concat([courses_df, course_snap_df])

    courses_df = courses_df.drop_duplicates(subset=["courseGlobalId"])
    return cards_df, courses_df, all_holes_df


a, b, c = process_scorecard(scorecard)

# TODO courses DF needs to have the tees column normalized, maybe? Or just keep it as json data

print(a)
print(b)
print(c)
# df = process_last10_chip(last10chip)
# print(df)

# a = process_last10_approach(last10approach)
# # print(pd.DataFrame(a))
# pprint(a)
# # df = process_clubs(clubs)

# conn = DataframeToBase("golf.db")
# conn.create_table(df, "chipshots")
# conn.insert_dataframe(df, "chipshots")
# conn.close()
