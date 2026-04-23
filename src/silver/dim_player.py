import pandas as pd
from datetime import datetime


def build_dim_player(people_df):

    df = people_df.copy()

    # standardize names
    df.columns = df.columns.str.strip().str.lower()

    # ensure expected cols exist
    if "player_id" not in df.columns:
        df["player_id"] = range(1, len(df) + 1)

    if "player_name" not in df.columns:
        raise ValueError("player_name column missing in BRONZE_PEOPLE")

    # short name
    df["short_name"] = df["player_name"].apply(make_short_name)

    # optional future fields
    df["active_flag"] = 1
    df["created_ts"] = datetime.now()

    return df[
        [
            "player_id",
            "player_name",
            "short_name",
            "active_flag",
            "created_ts"
        ]
    ]


def make_short_name(name):

    if pd.isna(name):
        return None

    parts = str(name).strip().split()

    if len(parts) == 1:
        return parts[0]

    initials = "".join([p[0] for p in parts[:-1]])

    return f"{initials} {parts[-1]}"