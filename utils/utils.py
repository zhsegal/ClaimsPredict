import pandas as pd
from datetime import datetime

def parse_date_column (df, date_columns):
    df[date_columns] = [datetime.strptime(date, '%Y%m%d') for date in df[date_columns].astype(int).astype(str)]
    return df


if __name__ == '__main__':
    create_code_symptom_mapping(pd.read_csv('Data/icd9dx2010.csv'),)