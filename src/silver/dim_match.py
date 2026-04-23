import json
import pandas as pd


def build_dim_match(bronze_df):

    rows = []

    for _, rec in bronze_df.iterrows():

        match_id = rec["match_id"]
        source_file = rec["source_file"]

        j = json.loads(rec["raw_json"])
        info = j.get("info", {})
        outcome = info.get("outcome", {})
        by = outcome.get("by", {})

        rows.append({
            "match_id": match_id,
            "source_file_name": source_file,
            "match_date": info.get("dates", [None])[0],
            "season": str(info.get("season")),
            "competition_name": info.get("event", {}).get("name"),
            "match_number": info.get("event", {}).get("match_number"),
            "stage": info.get("event", {}).get("stage"),
            "gender": info.get("gender"),
            "match_type": info.get("match_type"),
            "balls_per_over": info.get("balls_per_over"),
            "scheduled_overs_per_innings": info.get("overs"),
            "team1": info.get("teams", [None, None])[0],
            "team2": info.get("teams", [None, None])[1],
            "venue_name": info.get("venue"),
            "city": info.get("city"),
            "toss_winner": info.get("toss", {}).get("winner"),
            "toss_decision": info.get("toss", {}).get("decision"),
            "winner": outcome.get("winner"),
            "win_margin_runs": by.get("runs"),
            "win_margin_wickets": by.get("wickets")
        })

    return pd.DataFrame(rows)