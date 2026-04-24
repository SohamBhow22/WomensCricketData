import pandas as pd
from datetime import datetime


def build_fact_innings_summary(ball_df):

    df = ball_df.copy()

    agg_df = df.groupby(
        [
            "match_id",
            "innings_no",
            "batting_team_sk",
            "batting_team"
        ],
        dropna=False
    ).agg(
        runs_scored=("total_runs", "sum"),
        wickets_lost=("wicket_flag", "sum"),
        legal_balls=("is_legal_ball", "sum"),
        fours=("boundary_type", lambda x: (x == "FOUR").sum()),
        sixes=("boundary_type", lambda x: (x == "SIX").sum()),
        extras=("extras", "sum"),
        dot_balls=("is_dot_ball", "sum"),
        boundaries=("is_boundary", "sum"),

        powerplay_runs=("total_runs", lambda x: x[df.loc[x.index, "phase"] == "Powerplay"].sum()),
        middle_runs=("total_runs", lambda x: x[df.loc[x.index, "phase"] == "Middle"].sum()),
        death_runs=("total_runs", lambda x: x[df.loc[x.index, "phase"] == "Death"].sum()),

        powerplay_wickets=("wicket_flag", lambda x: x[df.loc[x.index, "phase"] == "Powerplay"].sum()),
        middle_wickets=("wicket_flag", lambda x: x[df.loc[x.index, "phase"] == "Middle"].sum()),
        death_wickets=("wicket_flag", lambda x: x[df.loc[x.index, "phase"] == "Death"].sum())
    ).reset_index()

    agg_df["overs_batted"] = round(
        agg_df["legal_balls"] / 6, 2
    )

    agg_df["run_rate"] = round(
        agg_df["runs_scored"] /
        agg_df["overs_batted"].replace(0, 1),
        2
    )

    agg_df["innings_id"] = (
        agg_df["match_id"].astype(str)
        + "_"
        + agg_df["innings_no"].astype(str)
    )

    agg_df["created_ts"] = datetime.now()

    final_cols = [
        "innings_id",
        "match_id",
        "innings_no",
        "batting_team_sk",
        "batting_team",
        "runs_scored",
        "wickets_lost",
        "legal_balls",
        "overs_batted",
        "run_rate",
        "fours",
        "sixes",
        "extras",
        "dot_balls",
        "boundaries",
        "powerplay_runs",
        "middle_runs",
        "death_runs",
        "powerplay_wickets",
        "middle_wickets",
        "death_wickets",
        "created_ts"
    ]

    return agg_df[final_cols]