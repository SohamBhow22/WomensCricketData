import pandas as pd
import hashlib
from datetime import datetime


def build_dim_player(people_df):

    df = people_df.copy()

    df.columns = df.columns.str.strip().str.lower()

    if "player_name" not in df.columns:
        raise ValueError("player_name column missing")

    # -----------------------------------
    # Clean data
    # -----------------------------------
    df["player_name"] = (
        df["player_name"]
        .astype(str)
        .str.strip()
    )

    df = df[df["player_name"] != ""].copy()

    # -----------------------------------
    # Business Key
    # -----------------------------------
    if "player_id" in df.columns:
        df["player_bk"] = (
            "P" + df["player_id"].astype(str)
        )
        df["source_player_id"] = df["player_id"]
    else:
        df["player_bk"] = (
            "NM_" +
            df["player_name"]
            .str.upper()
            .str.replace(" ", "_")
        )
        df["source_player_id"] = None

    # -----------------------------------
    # Deduplicate latest source rows
    # -----------------------------------
    df = df.drop_duplicates(
        subset=["player_bk", "player_name"]
    ).reset_index(drop=True)

    # -----------------------------------
    # Detect name history examples
    # For now create one current row only.
    # Later incremental loads create new SCD rows.
    # -----------------------------------
    now = datetime.now()

    df["player_sk"] = range(
        1,
        len(df) + 1
    )

    df["player_display_name"] = df["player_name"]

    df["short_name"] = df["player_name"].apply(
        make_short_name
    )

    df["active_flag"] = 1
    df["effective_from"] = now
    df["effective_to"] = pd.NaT
    df["is_current"] = 1

    df["record_hash"] = df.apply(
        lambda x: make_hash([
            x["player_bk"],
            x["player_name"],
            x["short_name"]
        ]),
        axis=1
    )

    df["created_ts"] = now
    df["updated_ts"] = now

    return df[
        [
            "player_sk",
            "player_bk",
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