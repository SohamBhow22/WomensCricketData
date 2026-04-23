import duckdb

conn = duckdb.connect("data/gold/cricket.db")

print(conn.execute("SHOW TABLES").fetchdf())

print(conn.execute("SELECT COUNT(*) FROM BRONZE_MATCHES").df())
print(conn.execute("SELECT * FROM BRONZE_MATCHES LIMIT 5").df())
print(conn.execute("SELECT * FROM BRONZE_PEOPLE LIMIT 10").df())
print(conn.execute("""
SELECT
    match_id,
    raw_json -> 'info' ->> 'venue' AS venue,
    raw_json -> 'info' ->> 'match_type' AS format
FROM BRONZE_MATCHES
""").df())

print(conn.execute("SELECT COUNT(*) FROM FACT_BALL_BY_BALL").fetchdf())
print(conn.execute("SELECT COUNT(*) FROM DIM_MATCH").fetchdf())
print(conn.execute("""
    SELECT match_id, innings_no, over_no, nth_over, ball_no, striker, bowler, total_runs
    FROM FACT_BALL_BY_BALL
    LIMIT 10
""").fetchdf())
print(conn.execute("SELECT * FROM DIM_PLAYER LIMIT 10;").fetchdf())
print(conn.execute("""
    SELECT player_id, player_name FROM DIM_PLAYER
    WHERE player_name = 'Mithali Raj'
""").fetchdf())
print(conn.execute("""SELECT * FROM FACT_PLAYER_MATCH_STATS LIMIT 20;""").fetchdf())
