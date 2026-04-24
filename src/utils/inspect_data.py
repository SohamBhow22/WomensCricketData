import duckdb
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)


def run_query(conn, title, sql):

    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)

    try:
        print(conn.execute(sql).fetchdf())
    except Exception as e:
        print(f"ERROR: {e}")


conn = duckdb.connect("data/gold/cricket.db")

# -----------------------------------
# Table Inventory
# -----------------------------------
run_query(
    conn,
    "ALL TABLES",
    """
    SHOW TABLES
    """
)

# -----------------------------------
# Bronze Layer
# -----------------------------------
run_query(
    conn,
    "BRONZE MATCH COUNT",
    """
    SELECT COUNT(*) AS total_matches
    FROM BRONZE_MATCHES
    """
)

run_query(
    conn,
    "BRONZE PEOPLE SAMPLE",
    """
    SELECT *
    FROM BRONZE_PEOPLE
    LIMIT 10
    """
)

# -----------------------------------
# Dimensions
# -----------------------------------
run_query(
    conn,
    "DIM_MATCH SAMPLE",
    """
    SELECT match_id, match_date, competition_name, match_type, event_sk
    FROM DIM_MATCH
    LIMIT 10
    """
)

run_query(
    conn,
    "DIM_PLAYER SAMPLE",
    """
    SELECT player_sk, player_bk, player_name, is_current
    FROM DIM_PLAYER
    LIMIT 20
    """
)

run_query(
    conn,
    "DIM_TEAM SAMPLE",
    """
    SELECT team_sk, team_name
    FROM DIM_TEAM
    LIMIT 20
    """
)

run_query(
    conn,
    "DIM_VENUE SAMPLE",
    """
    SELECT venue_sk, venue_name, city
    FROM DIM_VENUE
    LIMIT 20
    """
)

run_query(
    conn,
    "DIM_EVENT SAMPLE",
    """
    SELECT event_sk, competition_name_std, event_type, match_format
    FROM DIM_EVENT
    LIMIT 20
    """
)

# -----------------------------------
# Facts
# -----------------------------------
run_query(
    conn,
    "BALL FACT SAMPLE",
    """
    SELECT match_id, innings_no, over_no, ball_no, striker, bowler, total_runs
    FROM FACT_BALL_BY_BALL
    LIMIT 20
    """
)

run_query(
    conn,
    "INNINGS SUMMARY SAMPLE",
    """
    SELECT innings_id, match_id, innings_no, runs_scored, wickets_lost, run_rate
    FROM FACT_INNINGS_SUMMARY
    LIMIT 20
    """
)

run_query(
    conn,
    "MATCH SUMMARY SAMPLE",
    """
    SELECT match_id, batting_team_sk, venue_sk, total_runs, wickets_lost
    FROM FACT_MATCH_SUMMARY
    LIMIT 20
    """
)

run_query(
    conn,
    "PLAYER MATCH STATS SAMPLE",
    """
    SELECT *
    FROM FACT_PLAYER_MATCH_STATS
    LIMIT 20
    """
)

run_query(
    conn,
    "LOAD CONTROL STATUS",
    """
    SELECT load_id, batch_id, layer_name, load_status, started_ts, completed_ts
    FROM FACT_LOAD_CONTROL
    ORDER BY started_ts DESC
    """
)