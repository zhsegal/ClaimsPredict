import pandas as pd
import sqlite3
from utils.utils import parse_date_column
from utils.utils import time_from_string_to_int,db_name_from_table_name,time_from_sting
from config.configuration import Configuration
import json

class Dataset:
    def __init__(self):
        self.config = Configuration().get_config()
        self.train_end_time_int=time_from_string_to_int(self.config['experiment']['experiment'])
        self.train_end_time=time_from_sting((self.config['experiment']['experiment']))
        self.patient_id_columns = 'DESYNPUF_ID'
        self.beneficiary_db='DB/BENEFICIARY_08_TABLE.db'
        self.patient_id = 'DESYNPUF_ID'
        with open("Data/datasets_metadata.json", "r") as f:
            self.metadata = json.load(f)

    def get_string_from_ids (self,id_list, identifier, table_name):
        ids_quoted = ["'" + s + "'" for s in id_list]
        sql_list = "(" + ",".join(ids_quoted) + ")"
        sql_string = f"SELECT * from {table_name} WHERE {identifier} IN {sql_list}"

        return sql_string

    def get_table_with_mehod_name(self, table_type, methods):
        return f'{table_type}_{methods}_TABLE'

    def get_table_name(self, table_type):
        return f'{table_type}_TABLE'

    def get_lines_from_sql_by_id(self, ids, db_path, table_name):
        conn = sqlite3.connect(db_path)
        sql_query = self.get_string_from_ids(ids, self.patient_id_columns, table_name)
        data = pd.read_sql(sql_query, conn)
        conn.close()
        return data

    def get_lines_from_sql_by_id_and_date (self, ids, db_path, table_name, date_column, int_date, time_condition):
        conn = sqlite3.connect(db_path)
        id_query = self.get_string_from_ids(ids, self.patient_id_columns, table_name)
        time_sign='<' if time_condition=='past' else '>'
        time_query=f' AND {date_column} {time_sign} {int_date}'
        sql_query=id_query + time_query
        data = pd.read_sql(sql_query, conn)
        conn.close()
        return data

    def get_lines_from_sql_by_column(self, ids, db_path, table_name, columns):
        conn = sqlite3.connect(db_path)
        id_query = self.get_string_from_ids(ids, self.patient_id_columns, table_name)
        columns=[self.patient_id] + columns
        columns_query=', '.join(columns)
        sql_query=id_query.replace('*', columns_query)
        data = pd.read_sql(sql_query, conn)
        conn.close()
        return data


    def get_patient_ids(self):
        conn = sqlite3.connect(self.beneficiary_db)
        sql_query=f'SELECT {self.patient_id_columns} from BENEFICIARY_08_TABLE'
        data = pd.read_sql(sql_query, conn)
        conn.close()
        return data[self.patient_id_columns][:self.config['experiment']['patients_number']]




class InpatientDataset(Dataset):
    def __init__(self, method):
        super().__init__()
        self.table_type=self.metadata['datasets_data']['datasets_names']['inpatient']
        self.table_name=self.get_table_with_mehod_name(self.table_type, method)
        self.db_path=db_name_from_table_name(self.table_name)
        self.date_column=self.metadata['datasets_data']['datasets_date_column_names']['inpatient_hospitalziations'] if method==self.config['preprocessing']['method_names']['hospitalizations'] \
            else self.metadata['datasets_data']['datasets_date_column_names']['inpatient_claims']

    def get_patient_lines(self, id_list):
        data=self.get_lines_from_sql_by_id(id_list, self.db_path, self.table_name)
        data = parse_date_column(data, self.date_column)
        return data

    def get_patient_lines_in_train_time(self, id_list):
        data=self.get_lines_from_sql_by_id_and_date(id_list, self.db_path, self.table_name, self.date_column, self.train_end_time_int,'past')
        data = parse_date_column(data, self.date_column)
        return data

    def get_all_diagnosis_in_post_train_time(self, id_list):
        data=self.get_lines_from_sql_by_id_and_date(id_list, self.db_path, self.table_name, self.date_column, self.train_end_time_int,'post')
        data = parse_date_column(data, self.date_column)
        return data

class OutpatientDataset(Dataset):
    def __init__(self, method):
        super().__init__()
        self.table_type =self.metadata['datasets_data']['datasets_names']['outpatient']
        self.table_name = self.get_table_with_mehod_name(self.table_type, method)
        self.db_path = db_name_from_table_name(self.table_name)
        self.date_column = self.metadata['datasets_data']['datasets_date_column_names']['outpatient']

    def get_patient_lines(self, id_list):
        data=self.get_lines_from_sql_by_id(id_list, self.db_path, self.table_name)
        data=data.dropna(subset=[self.date_column])
        data = parse_date_column(data, self.date_column)
        return data

    def get_patient_lines_in_train_time(self, id_list):
        data = self.get_lines_from_sql_by_id_and_date(id_list, self.db_path, self.table_name, self.date_column,
                                                      self.train_end_time_int, 'past')
        data = parse_date_column(data, self.date_column)
        return data

    def get_all_diagnosis_in_post_train_time(self, id_list):
        data = self.get_lines_from_sql_by_id_and_date(id_list, self.db_path, self.table_name, self.date_column,
                                                      self.train_end_time_int, 'post')
        data = parse_date_column(data, self.date_column)
        return data

class CarrierDataset(Dataset):
    def __init__(self, method):
        super().__init__()
        self.table_type = self.metadata['datasets_data']['datasets_names']['carrier']
        self.table_name = self.get_table_with_mehod_name(self.table_type, method)
        self.db_path = db_name_from_table_name(self.table_name)
        self.date_column = self.metadata['datasets_data']['datasets_date_column_names']['carrier']

    def get_patient_lines(self, id_list):
        data=self.get_lines_from_sql_by_id(id_list, self.db_path, self.table_name)
        data=data.dropna(subset=[self.date_column])
        data = parse_date_column(data, self.date_column)
        return data

    def get_patient_lines_in_train_time(self, id_list):
        data = self.get_lines_from_sql_by_id_and_date(id_list, self.db_path, self.table_name, self.date_column,
                                                      self.train_end_time_int, 'past')
        data = parse_date_column(data, self.date_column)
        return data

    def get_all_diagnosis_in_post_train_time(self, id_list):
        data = self.get_lines_from_sql_by_id_and_date(id_list, self.db_path, self.table_name, self.date_column,
                                                      self.train_end_time_int, 'post')
        data = parse_date_column(data, self.date_column)
        return data

class BeneficiaryDataset(Dataset):
    def __init__(self, year):
        super().__init__()
        self.table_type = self.metadata['datasets_data']['datasets_names']['beneficiary']
        self.table_name = self.get_table_with_mehod_name(self.table_type, year)
        self.db_path = db_name_from_table_name(self.table_name)
        self.date_column=self.metadata['datasets_data']['datasets_date_column_names']['beneficiary']
        self.cat_columns = self.metadata['raw_tables_columns']['beneficiary_cat_columns']
        self.demographics_columns=self.metadata['raw_tables_columns']['beneficiary_demographics_columns']
        self.cost_columns=self.metadata['raw_tables_columns']['beneficiary_cost_columns']

    def get_patient_demographics_lines(self, id_list):
        data=self.get_lines_from_sql_by_column(id_list, self.db_path, self.table_name, self.demographics_columns)
        data = parse_date_column(data, self.date_column)
        data[self.cat_columns]=data[self.cat_columns].replace(2,0).replace('Y',1)
        return data

    def get_patient_costs_lines(self, id_list):
        data=self.get_lines_from_sql_by_column(id_list, self.db_path, self.table_name, self.cost_columns)

        return data


    def get_patient_lines(self, id_list):
        data=self.get_lines_from_sql_by_id(id_list, self.db_path, self.table_name)
        data=data.dropna(subset=[self.date_column])
        data = parse_date_column(data, self.date_column)
        return data

class MedicationsDataset(Dataset):
    def __init__(self):
        super().__init__()
        self.table_type = self.metadata['datasets_data']['datasets_names']['medications']
        self.table_name = self.get_table_name(self.table_type)
        self.db_path = db_name_from_table_name(self.table_name)
        self.date_column = self.metadata['datasets_data']['datasets_date_column_names']['medications']

    def get_patient_lines(self, id_list):
        data = self.get_lines_from_sql_by_id(id_list, self.db_path, self.table_name)
        data = data.dropna(subset=[self.date_column])
        data = parse_date_column(data, self.date_column)
        return data

    def get_patient_lines_in_train_time(self, id_list):
        data = self.get_lines_from_sql_by_id_and_date(id_list, self.db_path, self.table_name, self.date_column,
                                                      self.train_end_time_int, 'past')
        data = parse_date_column(data, self.date_column)
        return data

    def get_all_diagnosis_in_post_train_time(self, id_list):
        data = self.get_lines_from_sql_by_id_and_date(id_list, self.db_path, self.table_name, self.date_column,
                                                      self.train_end_time_int, 'post')
        data = parse_date_column(data, self.date_column)
        return data

if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    ids=['00013D2EFD8E45D1','00016F745862898F']

    InpatientDataset('DIAG').get_patient_lines(ids)