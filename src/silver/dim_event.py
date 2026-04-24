import pandas as pd
import hashlib
from datetime import datetime


def build_dim_event(match_df):

    df = match_df.copy()

    now = datetime.now()

    df["competition_name_raw"] = (
        df["competition_name"]
        .fillna("UNKNOWN")
        .astype(str)
        .str.strip()
    )

    df["competition_name_std"] = df[
        "competition_name_raw"
    ].apply(standardize_event_name)

    df["event_type"] = df[
        "competition_name_std"
    ].apply(get_event_type)

    df["is_international"] = df[
        "competition_name_std"
    ].apply(get_is_international)

    df["franchise_flag"] = df[
        "competition_name_std"
    ].apply(get_franchise_flag)

    df["match_format"] = df[
        "match_type"
    ].fillna("UNKNOWN")

    grp = df.groupby(
        [
            "competition_name_raw",
            "competition_name_std",
            "event_type",
            "match_format",
            "gender",
            "is_international",
            "franchise_flag"
        ],
        dropna=False
    ).agg(
        season_first_seen=("season", "min"),
        season_last_seen=("season", "max")
    ).reset_index()

    grp["event_sk"] = grp.apply(
        lambda x: make_event_sk(
            x["competition_name_std"],
            x["match_format"]
        ),
        axis=1
    )

    grp["is_current"] = 1
    grp["created_ts"] = now
    grp["updated_ts"] = now

    final_cols = [
        "event_sk",
        "competition_name_raw",
        "competition_name_std",
        "event_type",
        "match_format",
        "gender",
        "is_international",
        "franchise_flag",
        "season_first_seen",
        "season_last_seen",
        "is_current",
        "created_ts",
        "updated_ts"
    ]

    return grp[final_cols]


def standardize_event_name(name):

    txt = str(name).strip().upper()

    mapping = {
        "WBBL": "WOMEN'S BIG BASH LEAGUE",
        "WOMENS BIG BASH LEAGUE": "WOMEN'S BIG BASH LEAGUE",
        "THE HUNDRED WOMEN": "THE HUNDRED WOMEN",
        "WOMEN'S T20 WORLD CUP": "ICC WOMEN'S T20 WORLD CUP",
        "ICC WOMEN'S T20 WC": "ICC WOMEN'S T20 WORLD CUP",
        "ICC WOMEN'S T20 WORLD CUP": "ICC WOMEN'S T20 WORLD CUP",
        "WOMEN'S ODI WORLD CUP": "ICC WOMEN'S ODI WORLD CUP"
    }

    return mapping.get(txt, str(name).strip())


def get_event_type(name):

    txt = str(name).upper()

    if "WORLD CUP" in txt:
        return "WORLD_EVENT"

    if "LEAGUE" in txt or "WBBL" in txt:
        return "LEAGUE"

    if "HUNDRED" in txt:
        return "LEAGUE"

    if "SERIES" in txt:
        return "BILATERAL_SERIES"

    return "OTHER"


def get_is_international(name):

    txt = str(name).upper()

    if "WORLD CUP" in txt or "ASIA CUP" in txt:
        return 1

    return 0


def get_franchise_flag(name):

    txt = str(name).upper()

    if "LEAGUE" in txt or "HUNDRED" in txt or "WBBL" in txt:
        return 1

    return 0


def make_event_sk(name, fmt):

    txt = f"{name}|{fmt}"

    return "E" + hashlib.md5(
        txt.encode()
    ).hexdigest()[:8]