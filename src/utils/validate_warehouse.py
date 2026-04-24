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
# Duplicate Checks
# -----------------------------------
run_query(
    conn,
    "DUPLICATE BALL IDS",
    """
    SELECT ball_id, COUNT(*) AS cnt
    FROM FACT_BALL_BY_BALL
    GROUP BY ball_id
    HAVING COUNT(*) > 1
    """
)

run_query(
    conn,
    "DUPLICATE INNINGS IDS",
    """
    SELECT innings_id, COUNT(*) AS cnt
    FROM FACT_INNINGS_SUMMARY
    GROUP BY innings_id
    HAVING COUNT(*) > 1
    """
)

run_query(
    conn,
    "DUPLICATE MATCH IDS",
    """
    SELECT match_id, COUNT(*) AS cnt
    FROM DIM_MATCH
    GROUP BY match_id
    HAVING COUNT(*) > 1
    """
)

# -----------------------------------
# Null Foreign Key Checks
# -----------------------------------
run_query(
    conn,
    "MATCHES MISSING EVENT KEY",
    """
    SELECT match_id, competition_name, match_type
    FROM DIM_MATCH
    WHERE event_sk IS NULL
    """
)

run_query(
    conn,
    "BALL FACT NULL TEAM KEYS",
    """
    SELECT COUNT(*) AS missing_team_keys
    FROM FACT_BALL_BY_BALL
    WHERE batting_team_sk IS NULL
    """
)

run_query(
    conn,
    "BALL FACT NULL STRIKER KEYS",
    """
    SELECT COUNT(*) AS missing_striker_keys
    FROM FACT_BALL_BY_BALL
    WHERE striker_player_sk IS NULL
    """
)

run_query(
    conn,
    "BALL FACT NULL BOWLER KEYS",
    """
    SELECT COUNT(*) AS missing_bowler_keys
    FROM FACT_BALL_BY_BALL
    WHERE bowler_player_sk IS NULL
    """
)

# -----------------------------------
# Business Rule Checks
# -----------------------------------
run_query(
    conn,
    "INNINGS WITH INVALID WICKETS",
    """
    SELECT innings_id, wickets_lost
    FROM FACT_INNINGS_SUMMARY
    WHERE wickets_lost > 10
    """
)

run_query(
    conn,
    "MATCHES WITH MORE THAN 2 INNINGS",
    """
    SELECT match_id, COUNT(*) AS innings_count
    FROM FACT_INNINGS_SUMMARY
    GROUP BY match_id
    HAVING COUNT(*) > 2
    """
)

run_query(
    conn,
    "NEGATIVE RUNS IN BALL FACT",
    """
    SELECT match_id, innings_no, over_no, ball_no, total_runs
    FROM FACT_BALL_BY_BALL
    WHERE total_runs < 0
    """
)

# -----------------------------------
# Reconciliation Checks
# -----------------------------------
run_query(
    conn,
    "BALL VS INNINGS TOTALS MISMATCH",
    """
    SELECT b.match_id, b.innings_no, b.ball_runs, i.runs_scored
    FROM (
        SELECT match_id, innings_no, SUM(total_runs) AS ball_runs
        FROM FACT_BALL_BY_BALL
        GROUP BY match_id, innings_no
    ) b
    JOIN FACT_INNINGS_SUMMARY i
        ON b.match_id = i.match_id
       AND b.innings_no = i.innings_no
    WHERE b.ball_runs <> i.runs_scored
    """
)

run_query(
    conn,
    "UNMATCHED PLAYER NAMES ALL ROLES",
    """
    SELECT role_type, player_name, occurrences
    FROM (
        SELECT 'striker' AS role_type, striker AS player_name, COUNT(*) AS occurrences
        FROM FACT_BALL_BY_BALL
        WHERE striker_player_sk IS NULL
        GROUP BY striker

        UNION ALL

        SELECT 'non_striker' AS role_type, non_striker AS player_name, COUNT(*) AS occurrences
        FROM FACT_BALL_BY_BALL
        WHERE non_striker_player_sk IS NULL
        GROUP BY non_striker

        UNION ALL

        SELECT 'bowler' AS role_type, bowler AS player_name, COUNT(*) AS occurrences
        FROM FACT_BALL_BY_BALL
        WHERE bowler_player_sk IS NULL
        GROUP BY bowler
    ) x
    ORDER BY occurrences DESC, role_type
    """
)

# -----------------------------------
# Load Control
# -----------------------------------
run_query(
    conn,
    "FAILED LOADS",
    """
    SELECT load_id, batch_id, source_file, error_message
    FROM FACT_LOAD_CONTROL
    WHERE load_status = 'FAILED'
    ORDER BY started_ts DESC
    """
)

run_query(
    conn,
    "LATEST LOAD STATUS",
    """
    SELECT load_id, batch_id, layer_name, load_status, started_ts, completed_ts
    FROM FACT_LOAD_CONTROL
    ORDER BY started_ts DESC
    LIMIT 10
    """
)