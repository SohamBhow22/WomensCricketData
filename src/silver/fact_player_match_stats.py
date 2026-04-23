import pandas as pd


def build_fact_player_match_stats(ball_df):

    # -------------------------
    # Batting Stats
    # -------------------------
    batting = ball_df.groupby(
        ["match_id", "striker"],
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

    batting.rename(columns={"striker": "player_name"}, inplace=True)

    # -------------------------
    # Bowling Stats
    # -------------------------
    bowling = ball_df.groupby(
        ["match_id", "bowler"],
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

    bowling.rename(columns={"bowler": "player_name"}, inplace=True)

    # -------------------------
    # Merge batting + bowling
    # -------------------------
    final_df = pd.merge(
        batting,
        bowling,
        on=["match_id", "player_name"],
        how="outer"
    ).fillna(0)

    return final_df