import hashlib
from datetime import datetime


def create_load_control_table(conn):

    conn.execute("""
        CREATE TABLE IF NOT EXISTS FACT_LOAD_CONTROL (
            load_id BIGINT,
            batch_id VARCHAR,
            layer_name VARCHAR,
            source_file VARCHAR,
            file_hash VARCHAR,
            load_status VARCHAR,
            records_loaded BIGINT,
            started_ts TIMESTAMP,
            completed_ts TIMESTAMP,
            duration_seconds DOUBLE,
            error_message VARCHAR,
            created_ts TIMESTAMP,
            updated_ts TIMESTAMP
        )
    """)


def get_next_load_id(conn):

    return conn.execute("""
        SELECT COALESCE(MAX(load_id), 0) + 1
        FROM FACT_LOAD_CONTROL
    """).fetchone()[0]


def get_batch_id():

    return datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )


def get_file_hash(text_value):

    return hashlib.md5(
        str(text_value).encode()
    ).hexdigest()


def log_load_start(
    conn,
    load_id,
    batch_id,
    layer_name,
    source_file,
    file_hash
):

    now = datetime.now()

    conn.execute("""
        INSERT INTO FACT_LOAD_CONTROL
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        load_id,
        batch_id,
        layer_name,
        source_file,
        file_hash,
        "STARTED",
        None,
        now,
        None,
        None,
        None,
        now,
        now
    ])


def log_load_end(
    conn,
    load_id,
    status,
    records_loaded=0,
    error_message=None
):

    end_ts = datetime.now()

    start_ts = conn.execute("""
        SELECT started_ts
        FROM FACT_LOAD_CONTROL
        WHERE load_id = ?
    """, [load_id]).fetchone()[0]

    duration = (
        end_ts - start_ts
    ).total_seconds()

    conn.execute("""
        UPDATE FACT_LOAD_CONTROL
        SET load_status = ?,
            records_loaded = ?,
            completed_ts = ?,
            duration_seconds = ?,
            error_message = ?,
            updated_ts = ?
        WHERE load_id = ?
    """, [
        status,
        records_loaded,
        end_ts,
        duration,
        error_message,
        end_ts,
        load_id
    ])