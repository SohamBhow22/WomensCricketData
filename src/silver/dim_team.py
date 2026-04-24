import pandas as pd
from datetime import datetime


def build_dim_team(match_df):

    # collect teams from team1/team2
    teams_1 = match_df[["team1"]].rename(columns={"team1": "team_name"})
    teams_2 = match_df[["team2"]].rename(columns={"team2": "team_name"})

    df = pd.concat([teams_1, teams_2], ignore_index=True)

    df["team_name"] = df["team_name"].astype(str).str.strip()

    df = df.dropna(subset=["team_name"])
    df = df.drop_duplicates().reset_index(drop=True)

    # surrogate key
    df["team_sk"] = range(1, len(df) + 1)

    # natural key
    df["team_nk"] = df["team_name"].str.upper()

    df["team_display_name"] = df["team_name"]

    df["gender"] = "female"
    df["team_type"] = "team"

    df["is_active"] = 1

    now = datetime.now()

    df["effective_from"] = now
    df["effective_to"] = pd.NaT
    df["is_current"] = 1

    df["created_ts"] = now
    df["updated_ts"] = now

    return df[
        [
            "team_sk",
            "team_nk",
            "team_name",
            "team_display_name",
            "gender",
            "team_type",
            "is_active",
            "effective_from",
            "effective_to",
            "is_current",
            "created_ts",
            "updated_ts"
        ]
    ]