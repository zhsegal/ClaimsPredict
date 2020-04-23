import pandas as pd
import sqlite3
from Preprocessing import utils

class Dataset:
    def __init__(self):
        self.patient_id_columns='DESYNPUF_ID'

    def get_string_from_ids (self,id_list, identifier, table_name):
        ids_quoted = ["'" + s + "'" for s in id_list]
        sql_list = "(" + ",".join(ids_quoted) + ")"
        sql_string = f"SELECT * from {table_name} WHERE {identifier} IN {sql_list}"

        return sql_string

    def get_table_name(self, table_type, methods):
        return f'{table_type}_{methods}_TABLE'

class InpatientDataset(Dataset):
    def __init__(self, method):
        super().__init__()
        self.table_type='INPATIENT'
        self.table_name=self.get_table_name(self.table_type, method)
        self.db_path=f'DB/{self.table_name}.db'


    def get_patient_lines(self, id_list):
        conn= sqlite3.connect(self.db_path)
        sql_query = self.get_string_from_ids(id_list,self.patient_id_columns,self.table_name)
        data=  pd.read_sql(sql_query, conn)
        return data
        conn.close()


if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    ids=['00013D2EFD8E45D1','00016F745862898F']

    InpatientDataset('DIAG').get_patient_lines(ids)