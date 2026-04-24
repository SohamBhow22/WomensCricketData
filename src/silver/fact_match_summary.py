import pandas as pd
from datetime import datetime


def build_fact_match_summary(
    ball_df,
    match_df,
    team_df,
    venue_df
):

    df = ball_df.copy()

    # ---------------------------------
    # Aggregate innings summary
    # ---------------------------------
    summary = df.groupby(
        ["match_id", "innings_no", "batting_team"],
        as_index=False
    ).agg(
        total_runs=("total_runs", "sum"),
        wickets_lost=("wicket_flag", "sum"),
        legal_balls=("is_legal_ball", "sum"),
        boundaries=("is_boundary", "sum"),
        sixes=("boundary_type", lambda x: (x == "SIX").sum()),
        extras=("extras", "sum"),
        dismissals=("player_out", lambda x: x.notna().sum())
    )

    # ---------------------------------
    # Derived metrics
    # ---------------------------------
    summary["overs_batted"] = (
        summary["legal_balls"] / 6
    ).round(1)

    summary["run_rate"] = (
        summary["total_runs"] /
        summary["overs_batted"].replace(0, 1)
    ).round(2)

    # ---------------------------------
    # Add match date + venue
    # ---------------------------------
    summary = summary.merge(
        match_df[
            ["match_id", "match_date", "venue_name"]
        ],
        on="match_id",
        how="left"
    )

    # ---------------------------------
    # Join Team SK
    # Current version:
    # simple team_name join
    # Future:
    # effective date join
    # ---------------------------------
    summary = summary.merge(
        team_df[
            ["team_sk", "team_name"]
        ],
        left_on="batting_team",
        right_on="team_name",
        how="left"
    )

    # ---------------------------------
    # Join Venue SK
    # ---------------------------------
    summary = summary.merge(
        venue_df[
            ["venue_sk", "venue_name"]
        ],
        on="venue_name",
        how="left"
    )

    # ---------------------------------
    # Audit column
    # ---------------------------------
    summary["created_ts"] = datetime.now()

    # ---------------------------------
    # Final columns
    # ---------------------------------
    return summary[
        [
            "match_id",
            "innings_no",
            "match_date",
            "team_sk",
            "venue_sk",
            "total_runs",
            "wickets_lost",
            "legal_balls",
            "overs_batted",
            "run_rate",
            "boundaries",
            "sixes",
            "extras",
            "dismissals",
            "created_ts"
        ]
    ].rename(
        columns={
            "team_sk": "batting_team_sk"
        }
    )