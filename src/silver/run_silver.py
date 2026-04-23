from src.silver.fact_ball_by_ball import build_fact_ball_by_ball
from src.silver.dim_match import build_dim_match
from src.silver.dim_player import build_dim_player
from src.silver.fact_player_match_stats import build_fact_player_match_stats


def run_silver_layer(conn):

    bronze_matches = conn.execute("""
        SELECT match_id, source_file, raw_json
        FROM BRONZE_MATCHES
    """).fetchdf()

    bronze_people = conn.execute("""
        SELECT *
        FROM BRONZE_PEOPLE
    """).fetchdf()

    df_ball = build_fact_ball_by_ball(bronze_matches)
    df_match = build_dim_match(bronze_matches)
    df_player = build_dim_player(bronze_people)
    df_stats = build_fact_player_match_stats(df_ball)

    conn.register("df_ball", df_ball)
    conn.register("df_match", df_match)
    conn.register("df_player", df_player)
    conn.register("df_stats", df_stats)

    conn.execute("""
        CREATE OR REPLACE TABLE FACT_BALL_BY_BALL AS
        SELECT * FROM df_ball
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE DIM_MATCH AS
        SELECT * FROM df_match
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE DIM_PLAYER AS
        SELECT * FROM df_player
    """)

    conn.execute("""
    CREATE OR REPLACE TABLE FACT_PLAYER_MATCH_STATS AS
    SELECT * FROM df_stats
    """)

    print("Silver Layer completed.")