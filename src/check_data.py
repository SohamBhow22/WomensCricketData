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