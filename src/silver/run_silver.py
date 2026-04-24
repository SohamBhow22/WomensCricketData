from src.silver.fact_ball_by_ball import build_fact_ball_by_ball
from src.silver.dim_match import build_dim_match
from src.silver.dim_player import build_dim_player
from src.silver.fact_player_match_stats import build_fact_player_match_stats
from src.silver.dim_team import build_dim_team
from src.silver.dim_venue import build_dim_venue
from src.silver.fact_match_summary import build_fact_match_summary

def run_silver_layer(conn):

    bronze_matches = conn.execute("""
        SELECT match_id, source_file, raw_json
        FROM BRONZE_MATCHES
    """).fetchdf()

    bronze_people = conn.execute("""
        SELECT *
        FROM BRONZE_PEOPLE
    """).fetchdf()


    df_match = build_dim_match(bronze_matches)
    df_player = build_dim_player(bronze_people)
    df_team = build_dim_team(df_match)
    df_venue = build_dim_venue(df_match)
    df_ball = build_fact_ball_by_ball(
        bronze_matches, df_player, df_team
    )
    df_stats = build_fact_player_match_stats(
        df_ball, df_player
    )
    df_match_summary = build_fact_match_summary(
        df_ball, df_match, df_team, df_venue
    )

    conn.register("df_ball", df_ball)
    conn.register("df_match", df_match)
    conn.register("df_player", df_player)
    conn.register("df_stats", df_stats)
    conn.register("df_team", df_team)
    conn.register("df_venue", df_venue)
    conn.register("df_match_summary", df_match_summary)

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

    conn.execute("""
        CREATE OR REPLACE TABLE DIM_TEAM AS
        SELECT * FROM df_team
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE DIM_VENUE AS
        SELECT * FROM df_venue
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE FACT_MATCH_SUMMARY AS
        SELECT * FROM df_match_summary
    """)

    print("Silver Layer completed.")