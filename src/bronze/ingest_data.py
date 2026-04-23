import duckdb
import pandas as pd
import json
import os
from pathlib import Path


def create_bronze_tables(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS BRONZE_MATCHES (
        match_id        VARCHAR PRIMARY KEY,
        source_file     VARCHAR,
        raw_json        JSON,
        ingestion_ts    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS BRONZE_PEOPLE (
        player_id     VARCHAR PRIMARY KEY,
        player_name   VARCHAR
    )
    """)


# def load_people(conn, people_path):
#     df = pd.read_csv(people_path)
#     df.columns = ["player_id", "player_name"]
#     print(df.columns)
#     print(df.head())

#     conn.execute("DELETE FROM BRONZE_PEOPLE")
#     conn.register("people_df", df)

#     conn.execute("""
#     INSERT INTO BRONZE_PEOPLE
#     SELECT * FROM people_df
#     """)

def load_people(conn, people_path):
    df = pd.read_csv(people_path)

    # Inspect once (optional)
    print("Columns:", df.columns)

    # Select correct columns
    df = df[["identifier", "name"]]

    # Rename
    df.columns = ["player_id", "player_name"]

    conn.execute("DELETE FROM BRONZE_PEOPLE")
    conn.register("people_df", df)

    conn.execute("""
        INSERT INTO BRONZE_PEOPLE
        SELECT * FROM people_df
    """)


def get_match_id(file_path):
    return os.path.splitext(os.path.basename(file_path))[0]


def load_matches(conn, matches_path):
    files = Path(matches_path).glob("*.json")
    inserted = 0

    for file in files:
        match_id = get_match_id(file)

        exists = conn.execute("""
            SELECT 1 FROM BRONZE_MATCHES WHERE match_id = ?
        """, [match_id]).fetchone()

        if exists:
            continue

        with open(file, "r") as f:
            data = json.load(f)

        conn.execute("""
            INSERT INTO BRONZE_MATCHES (match_id, source_file, raw_json)
            VALUES (?, ?, ?)
        """, [match_id, str(file), json.dumps(data)])

        inserted += 1

    print(f"Inserted {inserted} new matches")


def run_bronze_layer(conn, matches_path, people_path):
    create_bronze_tables(conn)
    load_people(conn, people_path)
    load_matches(conn, matches_path)