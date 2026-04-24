import pandas as pd
import hashlib
from datetime import datetime


def build_dim_player(people_df):

    df = people_df.copy()

    # -----------------------------------
    # standardize columns
    # -----------------------------------
    df.columns = df.columns.str.strip().str.lower()

    # expected source columns:
    # player_id, player_name

    if "player_name" not in df.columns:
        raise ValueError("player_name column missing")

    # -----------------------------------
    # clean names
    # -----------------------------------
    df["player_name"] = (
        df["player_name"]
        .astype(str)
        .str.strip()
    )

    df = df[df["player_name"] != ""]

    df = df.drop_duplicates(
        subset=["player_name"]
    ).reset_index(drop=True)

    # -----------------------------------
    # surrogate key
    # -----------------------------------
    df["player_sk"] = range(1, len(df) + 1)

    # -----------------------------------
    # natural key
    # -----------------------------------
    df["player_nk"] = (
        df["player_name"]
        .str.upper()
    )

    # -----------------------------------
    # source id
    # -----------------------------------
    if "player_id" in df.columns:
        df["source_player_id"] = df["player_id"]
    else:
        df["source_player_id"] = None

    # -----------------------------------
    # display columns
    # -----------------------------------
    df["player_display_name"] = df["player_name"]

    df["short_name"] = df["player_name"].apply(
        make_short_name
    )

    # -----------------------------------
    # SCD columns
    # -----------------------------------
    now = datetime.now()

    df["active_flag"] = 1
    df["effective_from"] = now
    df["effective_to"] = pd.NaT
    df["is_current"] = 1

    # -----------------------------------
    # record hash
    # -----------------------------------
    df["record_hash"] = df.apply(
        lambda x: make_hash([
            x["player_nk"],
            x["player_display_name"],
            x["short_name"]
        ]),
        axis=1
    )

    df["created_ts"] = now
    df["updated_ts"] = now

    # -----------------------------------
    # final select
    # -----------------------------------
    return df[
        [
            "player_sk",
            "player_nk",
            "source_player_id",
            "player_name",
            "player_display_name",
            "short_name",
            "active_flag",
            "effective_from",
            "effective_to",
            "is_current",
            "record_hash",
            "created_ts",
            "updated_ts"
        ]
    ]


def make_short_name(name):

    parts = str(name).split()

    if len(parts) == 1:
        return parts[0]

    initials = "".join(
        [p[0] for p in parts[:-1]]
    )

    return f"{initials} {parts[-1]}"


def make_hash(values):

    txt = "|".join(
        [str(v) for v in values]
    )

    return hashlib.md5(
        txt.encode()
    ).hexdigest()