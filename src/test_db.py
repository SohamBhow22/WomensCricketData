import duckdb

con = duckdb.connect("data/gold/cricket.db")

df = con.execute("SELECT * FROM BRONZE_PEOPLE LIMIT 5").fetchdf()

print(df.columns.tolist())