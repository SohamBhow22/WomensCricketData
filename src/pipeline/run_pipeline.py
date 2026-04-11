import duckdb
from src.config import config
from src.bronze.ingest_data import run_bronze_layer
# from src.silver.silver_transformations import run_silver_layer

DB_PATH = config.DB_PATH
MATCHES_PATH = config.MATCHES_PATH
PEOPLE_PATH = config.PEOPLE_PATH


def main():
    conn = duckdb.connect(DB_PATH)

    print("Running Bronze Layer...")
    run_bronze_layer(conn, MATCHES_PATH, PEOPLE_PATH)

    # print("Running Silver Layer...")
    # run_silver_layer(conn)

    print("Pipeline completed successfully!")


if __name__ == "__main__":
    main()