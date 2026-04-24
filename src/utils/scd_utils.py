import hashlib
from datetime import datetime


def make_hash(values):

    txt = "|".join([str(v) for v in values])

    return hashlib.md5(txt.encode()).hexdigest()


def scd_defaults(df):

    now = datetime.now()

    df["effective_from"] = now
    df["effective_to"] = None
    df["is_current"] = 1
    df["is_active"] = 1
    df["created_ts"] = now
    df["updated_ts"] = now

    return df