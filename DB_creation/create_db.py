import pandas as pd
import sqlite3
import json
from config.configuration import Configuration


class BaseDB:
    def __init__(self):
        self.config = Configuration().get_config()

        self.diag_columns_str=['ICD9_DGNS']
        self.proc_columns_str=['ICD9_PRCDR']
        self.hcpcs_columns_str=['HCPCS']

        self.diags_name=self.config['preprocessing']['method_names']['diagnosis']
        self.procs_name=self.config['preprocessing']['method_names']['procedure']
        self.hcpcs_name=self.config['preprocessing']['method_names']['HSPCS']
        self.hospitalizations_name=self.config['preprocessing']['method_names']['hospitalizations']

        with open("Data/db_columns_names.json", "r") as f:
            self.column_names_dict = json.load(f)

    def create_table(self, df, claim_columns, mehod_name,method_columns ):
        concat_df = pd.DataFrame(columns=[f'{mehod_name}_CODE']  + claim_columns)

        for method_column in [col for col in df.columns if any(substring in col for substring in method_columns)]:
            method_df = df[[method_column] + claim_columns]
            method_df = method_df.rename(columns={method_column: f'{mehod_name}_CODE'})
            method_df = method_df.dropna(subset=[f'{mehod_name}_CODE'])

            concat_df = pd.concat([concat_df, method_df], ignore_index=True)

        return (concat_df)





    def create_sql(self, df,table_name):

        with sqlite3.connect(f'DB/{table_name}.db') as conn:   # You can create a new database by changing the name within the quotes
            cursor = conn.cursor()  # The database will be saved in the location where your 'py' file is saved
            df.to_sql(table_name, conn, if_exists='append', index=False)

        print(f'uploaded {table_name} to db')

    def create_merged_sql(self, df1, df2, table_name):
        with sqlite3.connect(f'DB/{table_name}.db') as conn:   # You can create a new database by changing the name within the quotes
            cursor = conn.cursor()  # The database will be saved in the location where your 'py' file is saved

            df=pd.concat([df1,df2])
            df.to_sql(table_name, conn, if_exists='append', index=False)

        print(f'uploaded {table_name} to db')


class BeneficiaryDB(BaseDB):
    def __init__(self):
        super().__init__()
        self.table_name='BENEFICIARY'
        self.dataframe=pd.read_csv('Data/Raw_Data/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv')

    def make_sql(self):
        self.create_sql(self.dataframe, f'{self.table_name}_TABLE')

class OutpatientDB(BaseDB):
    def __init__(self):
        super().__init__()
        self.table_name='OUTPATIENT'
        self.dataframe=pd.read_csv('Data/Raw_Data/DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.csv')

    def make_sql(self):
        oupatient_claim_columns = ['DESYNPUF_ID', 'CLM_ID', 'SEGMENT', 'CLM_FROM_DT', 'CLM_THRU_DT', 'PRVDR_NUM', 'CLM_PMT_AMT', 'NCH_PRMRY_PYR_CLM_PD_AMT',
                                   'AT_PHYSN_NPI', 'OP_PHYSN_NPI', 'OT_PHYSN_NPI', 'NCH_BENE_BLOOD_DDCTBL_LBLTY_AM']

        diags = self.create_table(self.dataframe, oupatient_claim_columns,self.diags_name,self.diag_columns_str)
        procs = self.create_table(self.dataframe, oupatient_claim_columns,self.procs_name,self.proc_columns_str)
        hcpcs= self.create_table(self.dataframe, oupatient_claim_columns,self.hcpcs_name,self.hcpcs_columns_str)

        self.create_sql(diags, f'{self.table_name}_{self.diags_name}_TABLE')
        self.create_sql(procs, f'{self.table_name}_{self.procs_name}_TABLE')
        self.create_sql(hcpcs, f'{self.table_name}_{self.hcpcs_name}_TABLE')

class InpatientDB(BaseDB):
    def __init__(self):
        super().__init__()
        self.dataframe=pd.read_csv('Data/Raw_Data/DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.csv')
        self.table_name = 'INPATIENT'


    def make_sql(self):
        inpatient_diagnosis_columns = self.column_names_dict['inpatient_diagnosis_columns']
        inpatient_hospitalization_columns = self.column_names_dict['inpatient_hospitalization_columns']


        diags = self.create_table(self.dataframe, inpatient_diagnosis_columns, self.diags_name, self.diag_columns_str)
        procs = self.create_table(self.dataframe, inpatient_diagnosis_columns, self.procs_name, self.proc_columns_str)
        hcpcs = self.create_table(self.dataframe, inpatient_diagnosis_columns, self.hcpcs_name, self.hcpcs_columns_str)
        hospitalizations=self.dataframe[inpatient_hospitalization_columns]

        # self.create_sql(diags, f'{self.table_name}_{self.diags_name}_TABLE')
        # self.create_sql(procs, f'{self.table_name}_{self.procs_name}_TABLE')
        # self.create_sql(hcpcs, f'{self.table_name}_{self.hcpcs_name}_TABLE')
        self.create_sql(hospitalizations, f'{self.table_name}_{self.hospitalizations_name}_TABLE')


class CarrierDB(BaseDB):
    def __init__(self):
        super().__init__()
        self.dataframe1=pd.read_csv('Data/Raw_Data/DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.csv')
        self.dataframe2 = pd.read_csv('Data/Raw_Data/DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.csv')
        self.table_name = 'CARRIER'


    def make_sql(self):
        carrier_claim_columns = inpatient_diagnosis_columns = self.column_names_dict['carrier_claim_columns']

        diags1 = self.create_table(self.dataframe1, carrier_claim_columns, self.diags_name, self.diag_columns_str)
        hcpcs1 = self.create_table(self.dataframe1, carrier_claim_columns, self.hcpcs_name, self.hcpcs_columns_str)



        diags2 = self.create_table(self.dataframe2, carrier_claim_columns, self.diags_name, self.diag_columns_str)
        hcpcs2 = self.create_table(self.dataframe2, carrier_claim_columns, self.hcpcs_name, self.hcpcs_columns_str)


        self.create_merged_sql(diags1, diags2, f'{self.table_name}_{self.diags_name}_TABLE')
        self.create_merged_sql(hcpcs1, hcpcs2, f'{self.table_name}_{self.hcpcs_name}_TABLE')

        return None

if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    InpatientDB().make_sql()


    # DB_columns=self.create_DB_columns(diags)
    # #DB_columns='id integer PRIMARY KEY, name text NOT NULL'
    #
    # #c.execute('''CREATE TABLE INPATIENT({})'''.format(DB_columns))

    # def create_DB_columns(self, df):
    #     sql_columns=str()
    #     for col_name in df.columns:
    #         sql_columns=sql_columns + f'{col_name} ' + f'{("TEXT" if df[col_name].dtype==object else "REAL")} '
    #         print(sql_columns)

