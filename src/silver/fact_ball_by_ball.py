import json
import pandas as pd
from datetime import datetime


def build_fact_ball_by_ball(
    bronze_df,
    player_df,
    team_df
):

    rows = []

    for _, rec in bronze_df.iterrows():

        match_id = rec["match_id"]

        match_json = json.loads(
            rec["raw_json"]
        )

        innings_list = match_json.get(
            "innings", []
        )

        ball_in_match = 1

        for inn_idx, innings in enumerate(
            innings_list, start=1
        ):

            batting_team = innings.get(
                "team"
            )

            for over_data in innings.get(
                "overs", []
            ):

                over_no = over_data.get(
                    "over", 0
                )

                nth_over = over_no + 1

                deliveries = over_data.get(
                    "deliveries", []
                )

                for ball_idx, delivery in enumerate(
                    deliveries, start=1
                ):

                    runs = delivery.get(
                        "runs", {}
                    )

                    wickets = delivery.get(
                        "wickets", []
                    )

                    extras_map = delivery.get(
                        "extras", {}
                    )

                    runs_batter = runs.get(
                        "batter", 0
                    )

                    extras = runs.get(
                        "extras", 0
                    )

                    total_runs = runs.get(
                        "total", 0
                    )

                    wicket_flag = 1 if wickets else 0

                    wicket_type = (
                        wickets[0]["kind"]
                        if wickets else None
                    )

                    player_out = (
                        wickets[0]["player_out"]
                        if wickets else None
                    )

                    striker = delivery.get(
                        "batter"
                    )

                    non_striker = delivery.get(
                        "non_striker"
                    )

                    bowler = delivery.get(
                        "bowler"
                    )

                    rows.append({
                        "ball_id": f"{match_id}_{inn_idx}_{over_no}_{ball_idx}",
                        "match_id": match_id,
                        "innings_no": inn_idx,
                        "over_no": over_no,
                        "nth_over": nth_over,
                        "ball_no": ball_idx,
                        "ball_in_match": ball_in_match,

                        "batting_team": batting_team,

                        "striker": striker,
                        "non_striker": non_striker,
                        "bowler": bowler,

                        "runs_batter": runs_batter,
                        "extras": extras,
                        "total_runs": total_runs,

                        "is_boundary": 1 if runs_batter in [4, 6] else 0,
                        "boundary_type":
                            "FOUR" if runs_batter == 4
                            else "SIX" if runs_batter == 6
                            else None,

                        "is_dot_ball": 1 if total_runs == 0 else 0,

                        "is_legal_ball": 0 if (
                            "wides" in extras_map or
                            "noballs" in extras_map
                        ) else 1,

                        "phase":
                            "Powerplay" if over_no <= 5
                            else "Middle" if over_no <= 14
                            else "Death",

                        "wicket_flag": wicket_flag,
                        "wicket_type": wicket_type,
                        "player_out": player_out,
                        "wicket_credit_bowler": 
                            1 if (
                                wicket_flag == 1 and
                                wicket_type not in ["run out", "retired hurt", "obstructing the field"]
                            ) else 0,

                        "created_ts": datetime.now()
                    })

                    ball_in_match += 1

    df = pd.DataFrame(rows)

    # -----------------------------------
    # Join Player Keys
    # -----------------------------------
    p = player_df[
        ["player_sk", "player_name"]
    ]

    df = df.merge(
        p.rename(columns={
            "player_sk": "striker_player_sk",
            "player_name": "striker"
        }),
        on="striker",
        how="left"
    )

    df = df.merge(
        p.rename(columns={
            "player_sk": "non_striker_player_sk",
            "player_name": "non_striker"
        }),
        on="non_striker",
        how="left"
    )

    df = df.merge(
        p.rename(columns={
            "player_sk": "bowler_player_sk",
            "player_name": "bowler"
        }),
        on="bowler",
        how="left"
    )

    # -----------------------------------
    # Join Team Key
    # -----------------------------------
    t = team_df[
        ["team_sk", "team_name"]
    ]

    df = df.merge(
        t.rename(columns={
            "team_sk": "batting_team_sk",
            "team_name": "batting_team"
        }),
        on="batting_team",
        how="left"
    )

    return df