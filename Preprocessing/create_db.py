import pandas as pd
import sqlite3

class InpatientDB:
    def __init__(self):
        self.dataframe=pd.read_csv('Data/DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.csv', nrows=100)
        self.diag_code='DIAG_CODE'
        self.is_admiting_diag='IS_ADMITING_DIAG'

    def create_csv_table(self):
        df = self.dataframe

        inpatient_claim_columns = ['DESYNPUF_ID', 'CLM_ID', 'SEGMENT', 'CLM_FROM_DT', 'CLM_THRU_DT',
       'PRVDR_NUM', 'CLM_PMT_AMT', 'NCH_PRMRY_PYR_CLM_PD_AMT',
       'AT_PHYSN_NPI', 'OP_PHYSN_NPI', 'OT_PHYSN_NPI', 'CLM_ADMSN_DT',
       'CLM_PASS_THRU_PER_DIEM_AMT',
       'NCH_BENE_IP_DDCTBL_AMT', 'NCH_BENE_PTA_COINSRNC_LBLTY_AM',
       'NCH_BENE_BLOOD_DDCTBL_LBLTY_AM', 'CLM_UTLZTN_DAY_CNT',
       'NCH_BENE_DSCHRG_DT', 'CLM_DRG_CD']

        diag_columns_str=['ICD9','HCPCS']

        concat_df=pd.DataFrame(columns=[self.diag_code] + [self.is_admiting_diag] + inpatient_claim_columns)
        #concat_df.loc[0] = 0


        for diag_column in [col for col in df.columns if any(substring in col for substring in diag_columns_str)]:
            diag_df=df[[diag_column] + inpatient_claim_columns]
            diag_df=diag_df.rename(columns={diag_column:self.diag_code})
            diag_df=diag_df.dropna(subset=[self.diag_code])
            diag_df[self.is_admiting_diag]=1 if diag_column=='ADMTNG_ICD9_DGNS_CD' else 0
            concat_df=pd.concat([concat_df, diag_df], ignore_index=True)

        return (concat_df)

    def create_sql(self):
        diags=self.create_csv_table()
        conn = sqlite3.connect('DB/INPATIENT_TABLE.db')  # You can create a new database by changing the name within the quotes
        c = conn.cursor()  # The database will be saved in the location where your 'py' file is saved
        DB_columns=self.create_DB_columns(diags)
        DB_columns='id integer PRIMARY KEY, name text NOT NULL'
        c.execute('''CREATE TABLE INPATIENT({})'''.format(DB_columns))
        diags.to_sql('CLIENTS', conn, if_exists='append', index=False)

        print (self.dataframe.shape)

    def create_DB_columns(self, df):
        sql_columns=str()
        for col_name in df.columns:
            print(col_name)


if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    InpatientDB().create_sql()