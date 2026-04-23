import json
import pandas as pd


def build_fact_ball_by_ball(bronze_df):
    rows = []

    for _, rec in bronze_df.iterrows():

        match_id = rec["match_id"]
        match_json = json.loads(rec["raw_json"])

        innings_list = match_json.get("innings", [])

        ball_in_match = 1

        for inn_idx, innings in enumerate(innings_list, start=1):

            batting_team = innings.get("team")

            for over_data in innings.get("overs", []):

                over_no = over_data.get("over", 0)
                nth_over = over_no + 1

                deliveries = over_data.get("deliveries", [])

                for ball_idx, delivery in enumerate(deliveries, start=1):

                    runs = delivery.get("runs", {})
                    wickets = delivery.get("wickets", [])
                    extras_map = delivery.get("extras", {})

                    runs_batter = runs.get("batter", 0)
                    extras = runs.get("extras", 0)
                    total_runs = runs.get("total", 0)

                    wicket_flag = 1 if wickets else 0
                    wicket_type = wickets[0]["kind"] if wickets else None
                    player_out = wickets[0]["player_out"] if wickets else None

                    if "wides" in extras_map:
                        extra_type = "wide"
                    elif "noballs" in extras_map:
                        extra_type = "noball"
                    elif "byes" in extras_map:
                        extra_type = "bye"
                    elif "legbyes" in extras_map:
                        extra_type = "legbye"
                    else:
                        extra_type = None

                    if over_no <= 5:
                        phase = "Powerplay"
                    elif over_no <= 14:
                        phase = "Middle"
                    else:
                        phase = "Death"

                    rows.append({
                        "ball_id": f"{match_id}_{inn_idx}_{over_no}_{ball_idx}",
                        "match_id": match_id,
                        "innings_no": inn_idx,
                        "over_no": over_no,
                        "nth_over": nth_over,
                        "ball_no": ball_idx,
                        "ball_in_match": ball_in_match,
                        "batting_team": batting_team,
                        "striker": delivery.get("batter"),
                        "non_striker": delivery.get("non_striker"),
                        "bowler": delivery.get("bowler"),
                        "runs_batter": runs_batter,
                        "extras": extras,
                        "total_runs": total_runs,
                        "extra_type": extra_type,
                        "wicket_flag": wicket_flag,
                        "wicket_type": wicket_type,
                        "player_out": player_out,
                        "is_boundary": 1 if runs_batter in [4, 6] else 0,
                        "boundary_type": "FOUR" if runs_batter == 4 else "SIX" if runs_batter == 6 else None,
                        "is_dot_ball": 1 if total_runs == 0 else 0,
                        "is_legal_ball": 0 if extra_type in ["wide", "noball"] else 1,
                        "phase": phase,
                        "wicket_credit_bowler": 1 if wicket_flag and wicket_type not in ["run out", "retired hurt"] else 0,
                        "strike_rotated": 1 if total_runs in [1, 3, 5] else 0,
                        "pressure_ball": 1 if total_runs == 0 or wicket_flag else 0
                    })

                    ball_in_match += 1

    return pd.DataFrame(rows)