import duckdb

from src.config import config
from src.bronze.ingest_data import run_bronze_layer
from src.silver.run_silver import run_silver_layer

from src.control.load_control import (
    create_load_control_table,
    get_next_load_id,
    get_batch_id,
    get_file_hash,
    log_load_start,
    log_load_end
)

DB_PATH = config.DB_PATH
MATCHES_PATH = config.MATCHES_PATH
PEOPLE_PATH = config.PEOPLE_PATH


def main():

    conn = duckdb.connect(DB_PATH)

    create_load_control_table(conn)

    load_id = get_next_load_id(conn)

    batch_id = get_batch_id()

    file_signature = (
        str(MATCHES_PATH) +
        "|" +
        str(PEOPLE_PATH)
    )

    file_hash = get_file_hash(
        file_signature
    )

    log_load_start(
        conn,
        load_id,
        batch_id,
        "PIPELINE",
        "ALL_INPUTS",
        file_hash
    )

    try:

        print("Running Bronze Layer...")
        run_bronze_layer(
            conn,
            MATCHES_PATH,
            PEOPLE_PATH
        )

        print("Running Silver Layer...")
        run_silver_layer(conn)

        total_rows = conn.execute("""
            SELECT COUNT(*)
            FROM FACT_BALL_BY_BALL
        """).fetchone()[0]

        log_load_end(
            conn,
            load_id,
            "SUCCESS",
            total_rows
        )

        print(
            "Pipeline completed successfully!"
        )

    except Exception as e:

        log_load_end(
            conn,
            load_id,
            "FAILED",
            0,
            str(e)
        )

        raise e


if __name__ == "__main__":
    main()