import pandas as pd
from datetime import datetime
import requests
import json
from  xml.etree import ElementTree
from functools import reduce
import numpy as np


def get_rxcui_from_ndc(ndc):

    url=requests.get(f"https://rxnav.nlm.nih.gov/REST/ndcstatus?ndc={ndc}")
    root = ElementTree.fromstring(url.content)
    rxcui_num=[elem.text for elem in root.iter('rxcui')]
    rxcui_text=[elem.text for elem in root.iter('conceptName')]
    return(rxcui_num[0],rxcui_text[0])

def parse_date_column (df, date_columns):
    df[date_columns] = [datetime.strptime(date, '%Y%m%d') for date in df[date_columns].astype(int).astype(str)]
    return df

def time_from_string_to_int(string_time):
    return int(string_time.replace(":",""))

def time_from_sting(string_time):
    return datetime.strptime(string_time,'%Y:%m:%d')

def db_name_from_table_name(table_name):
    return f'DB/{table_name}.db'

def merge_dfs_on_column(dfs, column):
    merged=reduce(lambda left, right: pd.merge(left, right, on=column, how='outer'),dfs)
    return merged

def string_to_int_else_nan(columns):
    def intitizer(string):
        try:
            return(float(string))
        except:
            return np.nan

    return  [intitizer(string) for string in columns ]