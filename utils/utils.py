import pandas as pd
from datetime import datetime

def parse_date_column (df, date_columns):
    df[date_columns] = [datetime.strptime(date, '%Y%m%d') for date in df[date_columns].astype(int).astype(str)]
    return df

def time_from_string_to_int(string_time):
    return int(string_time.replace(":",""))




