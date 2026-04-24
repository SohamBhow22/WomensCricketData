import pandas as pd
from datetime import datetime


def build_dim_venue(match_df):

    df = match_df[["venue_name", "city"]].copy()

    df["venue_name"] = df["venue_name"].astype(str).str.strip()
    df["city"] = df["city"].fillna("Unknown").astype(str).str.strip()

    # remove blanks
    df = df[df["venue_name"] != ""]
    df = df.drop_duplicates().reset_index(drop=True)

    # surrogate key
    df["venue_sk"] = range(1, len(df) + 1)

    # natural key
    df["venue_nk"] = (
        df["venue_name"].str.upper() + "|" +
        df["city"].str.upper()
    )

    df["venue_display_name"] = df["venue_name"]

    # future enrichment
    df["country"] = None
    df["timezone"] = None

    df["is_active"] = 1

    now = datetime.now()

    df["effective_from"] = now
    df["effective_to"] = pd.NaT
    df["is_current"] = 1

    df["created_ts"] = now
    df["updated_ts"] = now

    return df[
        [
            "venue_sk",
            "venue_nk",
            "venue_name",
            "venue_display_name",
            "city",
            "country",
            "timezone",
            "is_active",
            "effective_from",
            "effective_to",
            "is_current",
            "created_ts",
            "updated_ts"
        ]
    ]