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
    SELECT player_sk, innings_no, over_no, nth_over, ball_no, striker, bowler, total_runs
    FROM FACT_BALL_BY_BALL
    LIMIT 10
""").fetchdf())
print(conn.execute("SELECT * FROM DIM_PLAYER LIMIT 10;").fetchdf())
print(conn.execute("""
    SELECT player_sk, player_name FROM DIM_PLAYER
    WHERE player_name = 'Mithali Raj'
""").fetchdf())
print(conn.execute("""SELECT * FROM FACT_PLAYER_MATCH_STATS LIMIT 20;""").fetchdf())
print(conn.execute("""SELECT * FROM DIM_TEAM ORDER BY team_name;""").fetchdf())
print(conn.execute("""SELECT * FROM DIM_VENUE LIMIT 10;""").fetchdf().iloc[:, :4])
print(conn.execute("""SELECT * FROM FACT_MATCH_SUMMARY LIMIT 10;""").fetchdf())
print(conn.execute("""SELECT player_sk, player_name, short_name, is_current FROM DIM_PLAYER LIMIT 20;""").fetchdf())