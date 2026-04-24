import pandas as pd
from datetime import datetime


def build_dim_player_alias(player_df):

    df = player_df.copy()

    now = datetime.now()

    rows = []

    alias_sk = 1

    for _, row in df.iterrows():

        if row["is_current"] != 1:
            continue

        player_name = str(
            row["player_name"]
        ).strip()

        player_bk = row["player_bk"]

        aliases = generate_aliases(
            player_name
        )

        for alias_name, method, score in aliases:

            rows.append({
                "alias_sk": alias_sk,
                "alias_name": alias_name,
                "alias_name_normalized": normalize_name(alias_name),
                "player_bk": player_bk,
                "canonical_player_name": player_name,
                "match_method": method,
                "confidence_score": score,
                "effective_from": now,
                "effective_to": pd.NaT,
                "is_current": 1,
                "created_ts": now,
                "updated_ts": now
            })

            alias_sk += 1

    out_df = pd.DataFrame(rows)

    out_df = out_df.drop_duplicates(
        subset=["alias_name_normalized", "player_bk"]
    ).reset_index(drop=True)

    out_df["alias_sk"] = range(
        1,
        len(out_df) + 1
    )

    return out_df


def generate_aliases(name):

    aliases = []

    clean_name = str(name).strip()

    # -----------------------------------
    # Exact source name
    # -----------------------------------
    aliases.append((
        clean_name,
        "exact_name",
        1.00
    ))

    # -----------------------------------
    # Remove dots
    # -----------------------------------
    no_dot = clean_name.replace(".", "")

    if no_dot != clean_name:

        aliases.append((
            no_dot,
            "remove_dot",
            0.99
        ))

    # -----------------------------------
    # Hyphen to space
    # -----------------------------------
    hyphen_space = clean_name.replace("-", " ")

    if hyphen_space != clean_name:

        aliases.append((
            hyphen_space,
            "hyphen_to_space",
            0.98
        ))

    # -----------------------------------
    # Remove hyphen fully
    # -----------------------------------
    hyphen_removed = clean_name.replace("-", "")

    if hyphen_removed != clean_name:

        aliases.append((
            hyphen_removed,
            "remove_hyphen",
            0.97
        ))

    # -----------------------------------
    # Collapse extra spaces
    # -----------------------------------
    collapsed = " ".join(
        clean_name.split()
    )

    if collapsed != clean_name:

        aliases.append((
            collapsed,
            "space_normalized",
            0.96
        ))

    return aliases


def normalize_name(name):

    return (
        str(name)
        .upper()
        .replace(".", "")
        .replace("-", " ")
        .strip()
    )