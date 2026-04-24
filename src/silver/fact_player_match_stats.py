import pandas as pd
from datetime import datetime


def build_fact_player_match_stats(
    ball_df,
    player_df
):

    # ---------------------------
    # Batting
    # ---------------------------
    batting = ball_df.groupby(
        ["match_id", "striker_player_sk"],
        as_index=False
    ).agg(
        runs_scored=("runs_batter", "sum"),
        balls_faced=("is_legal_ball", "sum"),
        fours=("boundary_type", lambda x: (x == "FOUR").sum()),
        sixes=("boundary_type", lambda x: (x == "SIX").sum()),
        dismissals=("player_out", lambda x: x.notna().sum())
    )

    batting["strike_rate"] = (
        batting["runs_scored"] * 100 /
        batting["balls_faced"].replace(0, 1)
    ).round(2)

    batting.rename(
        columns={
            "striker_player_sk": "player_sk"
        },
        inplace=True
    )

    # ---------------------------
    # Bowling
    # ---------------------------
    bowling = ball_df.groupby(
        ["match_id", "bowler_player_sk"],
        as_index=False
    ).agg(
        balls_bowled=("is_legal_ball", "sum"),
        runs_conceded=("total_runs", "sum"),
        wickets=("wicket_credit_bowler", "sum"),
        dot_balls=("is_dot_ball", "sum")
    )

    bowling["overs_bowled"] = (
        bowling["balls_bowled"] / 6
    ).round(1)

    bowling["economy"] = (
        bowling["runs_conceded"] /
        bowling["overs_bowled"].replace(0, 1)
    ).round(2)

    bowling.rename(
        columns={
            "bowler_player_sk": "player_sk"
        },
        inplace=True
    )

    # ---------------------------
    # Merge
    # ---------------------------
    final_df = batting.merge(
        bowling,
        on=["match_id", "player_sk"],
        how="outer"
    ).fillna(0)

    # ---------------------------
    # Add player name
    # ---------------------------
    final_df = final_df.merge(
        player_df[
            ["player_sk", "player_name"]
        ],
        on="player_sk",
        how="left"
    )

    final_df["created_ts"] = datetime.now()

    return final_df[
        [
            "match_id",
            "player_sk",
            "player_name",
            "runs_scored",
            "balls_faced",
            "strike_rate",
            "fours",
            "sixes",
            "dismissals",
            "balls_bowled",
            "overs_bowled",
            "runs_conceded",
            "wickets",
            "economy",
            "dot_balls",
            "created_ts"
        ]
    ]