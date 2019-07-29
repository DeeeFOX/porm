from datetime import datetime as dt


def date_type(dt_str, fmt='%Y%m%d'):
    return dt.strptime(dt_str, fmt)
