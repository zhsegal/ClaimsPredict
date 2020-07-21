import pandas as pd
import sqlite3
import json
from config.configuration import Configuration
from utils.utils import get_rxcui_from_ndc,merge_dfs_on_column

class BaseDB:
    def __init__(self):
        with open("Data/datasets_metadata.json", "r") as f:
            self.metadata = json.load(f)

        self.config = Configuration().get_config()

        self.diag_columns_str=['ICD9_DGNS']
        self.proc_columns_str=['ICD9_PRCDR']
        self.hcpcs_columns_str=['HCPCS']
        self.NCH_costs_columns_str=['NCH_PMT']
        self.deductable_costs_columns_srt=['DDCTBL_AMT']
        self.primary_care_costs_columns_srt = ['PRMRY_PYR_PD_AMT']
        self.coinsurance_costs_columns_srt = ['COINSRNC_AMT']
        self.allowed_charge_costs_columns_srt = ['ALOWD_CHRG']

        self.outpatient_db_title=self.get_db_name('outpatient')
        self.medications_db_title = self.get_db_name('medications')
        self.beneficifary_db_title = self.get_db_name('beneficiary')

        self.medicaion_db_name=f'{self.medications_db_title}_TABLE'
        self.beneficiary_db_name_08=f'{self.beneficifary_db_title}_08_TABLE'
        self.beneficiary_db_name_09 =f'{self.beneficifary_db_title}_09_TABLE'
        self.beneficiary_db_name_10 =f'{self.beneficifary_db_title}_10_TABLE'



        self.diags_name=self.get_method_name('diagnosis')
        self.procs_name=self.get_method_name('procedure')
        self.hcpcs_name=self.get_method_name('HSPCS')

        self.hospitalizations_name=self.config['preprocessing']['method_names']['hospitalizations']
        self.deductable_costs_name = self.config['preprocessing']['method_names']['deductible_costs']
        self.NCH_costs_columns_name = self.config['preprocessing']['method_names']['NCH_costs']
        self.primary_care_costs_name = self.config['preprocessing']['method_names']['primary_care_costs']
        self.coinsurance_costs_name = self.config['preprocessing']['method_names']['coinsurance_costs']
        self.allowed_charge_costs_name = self.config['preprocessing']['method_names']['allowd_chargae']
        self.inpatient_costs_name = self.config['preprocessing']['method_names']['inpatient_costs']
        self.outpatient_costs_name = self.config['preprocessing']['method_names']['outpatient_costs']





    def create_table(self, df, claim_columns, mehod_name,method_columns ):
        concat_df = pd.DataFrame(columns=[f'{mehod_name}_CODE']  + claim_columns)

        for method_column in [col for col in df.columns if any(substring in col for substring in method_columns)]:
            method_df = df[[method_column] + claim_columns]
            method_df = method_df.rename(columns={method_column: f'{mehod_name}_CODE'})
            method_df = method_df.dropna(subset=[f'{mehod_name}_CODE'])

            concat_df = pd.concat([concat_df, method_df], ignore_index=True)

        return (concat_df)

    def create_table_with_condition(self, df, claim_columns, mehod_name, method_columns,value):
        concat_df = pd.DataFrame(columns=[f'{mehod_name}']  + claim_columns)

        for method_column in [col for col in df.columns if any(substring in col for substring in method_columns)]:
            method_df = df[[method_column] + claim_columns]
            method_df = method_df.rename(columns={method_column: f'{mehod_name}'})
            method_df = method_df.dropna(subset=[f'{mehod_name}'])
            method_df=method_df[method_df[f'{mehod_name}']>value]

            concat_df = pd.concat([concat_df, method_df], ignore_index=True)

        return (concat_df)

    def create_sql(self, df, db_name):

        with sqlite3.connect(self.db_name_to_path(db_name)) as conn:
            cursor = conn.cursor()
            df.to_sql(db_name, conn, if_exists='append', index=False)

        print(f'uploaded {db_name} to db')

    def create_merged_sql(self, df1, df2, table_name):
        with sqlite3.connect(f'DB/{table_name}.db') as conn:   # You can create a new database by changing the name within the quotes
            cursor = conn.cursor()  # The database will be saved in the location where your 'py' file is saved

            df=pd.concat([df1,df2])
            df.to_sql(table_name, conn, if_exists='append', index=False)

        print(f'uploaded {table_name} to db')


    def db_name_to_path(self, db_name, db_folder='DB'):
        return f'{db_folder}/{db_name}.db'

    def get_db_name(self, db_name):
        return self.metadata['datasets_data']['datasets_names'][db_name]

    def get_method_name(self, method_name):
        return self.config['preprocessing']['method_names'][method_name]


class MedicationsDB(BaseDB):

    def __init__(self):
        super().__init__()
        self.dataframe = pd.read_csv('Data/Raw_Data/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_1.csv',nrows=100)
        self.ndc_columns='PROD_SRVC_ID'

    def make_sql(self):
        self.dataframe[self.ndc_columns]=[self.zero_pad(ndc) for ndc in self.dataframe[self.ndc_columns]]
        rxcui_tuples=[get_rxcui_from_ndc(ndc) for ndc in self.dataframe[self.ndc_columns]]
        self.dataframe['rxcui_num']=[tuple[0] for tuple in rxcui_tuples]
        self.dataframe['rxcui_description'] = [tuple[1] for tuple in rxcui_tuples]
        self.create_sql(self.dataframe, self.medicaion_db_name)
        pass

    def zero_pad(self,ndc):
        ndc=str(ndc)
        if len(ndc)==7:
            return ('0000' + ndc)
        elif len(ndc)==8:
            return ('000' + ndc)
        elif len(ndc)==9:
            return ('00' + ndc)
        elif len(ndc)==10:
            return ('0' + ndc)
        else:
            return ndc

class BeneficiaryDB(BaseDB):
    def __init__(self):
        super().__init__()
        self.dataframe_08=pd.read_csv('Data/Raw_Data/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv')
        self.dataframe_09 = pd.read_csv('Data/Raw_Data/DE1_0_2009_Beneficiary_Summary_File_Sample_1.csv')
        self.dataframe_10 = pd.read_csv('Data/Raw_Data/DE1_0_2010_Beneficiary_Summary_File_Sample_1.csv')

    def make_sql(self):

        self.create_sql(self.dataframe_08, self.beneficiary_db_name_08)
        self.create_sql(self.dataframe_09, self.beneficiary_db_name_09)
        self.create_sql(self.dataframe_10, self.beneficiary_db_name_10)



class OutpatientDB(BaseDB):
    def __init__(self):
        super().__init__()
        self.dataframe=pd.read_csv('Data/Raw_Data/DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.csv')

    def make_sql(self):
        oupatient_diagnosis_columns = self.metadata['raw_tables_columns']['outpatient_diagnosis_columns']
        oupatient_costs_columns = self.metadata['raw_tables_columns']['outpatient_cost_columns']

        diags = self.create_table(self.dataframe, oupatient_diagnosis_columns,self.diags_name,self.diag_columns_str)
        procs = self.create_table(self.dataframe, oupatient_diagnosis_columns,self.procs_name,self.proc_columns_str)
        hcpcs= self.create_table(self.dataframe, oupatient_diagnosis_columns,self.hcpcs_name,self.hcpcs_columns_str)
        costs=self.dataframe[oupatient_costs_columns]

        self.create_sql(diags, f'{self.outpatient_db_title}_{self.diags_name}_TABLE')
        self.create_sql(procs, f'{self.outpatient_db_title}_{self.procs_name}_TABLE')
        self.create_sql(hcpcs, f'{self.outpatient_db_title}_{self.hcpcs_name}_TABLE')
        self.create_sql(costs, f'{self.outpatient_db_title}_{self.outpatient_costs_name}_TABLE')


class InpatientDB(BaseDB):
    def __init__(self):
        super().__init__()
        self.dataframe=pd.read_csv('Data/Raw_Data/DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.csv')
        self.table_name = self.metadata['datasets_data']['datasets_names']['inpatient']
        self.date_column='CLM_FROM_DT'

    def make_sql(self):
        inpatient_diagnosis_columns = self.metadata['raw_tables_columns']['inpatient_diagnosis_columns']
        inpatient_hospitalization_columns = self.metadata['raw_tables_columns']['inpatient_hospitalization_columns']
        inpatient_cost_columns = self.metadata['raw_tables_columns']['inpatient_cost_columns']

        # diags = self.create_table(self.dataframe, inpatient_diagnosis_columns, self.diags_name, self.diag_columns_str)
        # procs = self.create_table(self.dataframe, inpatient_diagnosis_columns, self.procs_name, self.proc_columns_str)
        # hcpcs = self.create_table(self.dataframe, inpatient_diagnosis_columns, self.hcpcs_name, self.hcpcs_columns_str)
        # hospitalizations=self.dataframe[inpatient_hospitalization_columns]

        costs=self.dataframe[inpatient_cost_columns].dropna(subset=[self.date_column]).fillna(0)
        print ('tables created')

        # self.create_sql(diags, f'{self.table_name}_{self.diags_name}_TABLE')
        # self.create_sql(procs, f'{self.table_name}_{self.procs_name}_TABLE')
        # self.create_sql(hcpcs, f'{self.table_name}_{self.hcpcs_name}_TABLE')
        #self.create_sql(hospitalizations, f'{self.table_name}_{self.hospitalizations_name}_TABLE')
        self.create_sql(costs, f'{self.table_name}_{self.inpatient_costs_name}_TABLE')
        print ('all dbs created')

class CarrierDB(BaseDB):
    def __init__(self):
        super().__init__()
        self.dataframe1=pd.read_csv('Data/Raw_Data/DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.csv')
        self.dataframe2 = pd.read_csv('Data/Raw_Data/DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.csv')
        self.table_name =  self.metadata['datasets_data']['datasets_names']['carrier']


    def make_sql(self):
        carrier_method_columns = self.metadata['raw_tables_columns']['carrier_method_columns']
        carrier_cost_columns = self.metadata['raw_tables_columns']['carrier_cost_columns']

        # diags1 = self.create_table(self.dataframe1, carrier_method_columns, self.diags_name, self.diag_columns_str)
        procs1 = self.create_table(self.dataframe1, carrier_method_columns, self.procs_name, self.proc_columns_str)
        # hcpcs1 = self.create_table(self.dataframe1, carrier_method_columns, self.hcpcs_name, self.hcpcs_columns_str)
        #
        # diags2 = self.create_table(self.dataframe2, carrier_method_columns, self.diags_name, self.diag_columns_str)
        procs2 = self.create_table(self.dataframe2, carrier_method_columns, self.procs_name, self.proc_columns_str)
        # hcpcs2 = self.create_table(self.dataframe2, carrier_method_columns, self.hcpcs_name, self.hcpcs_columns_str)
        #
        # deductible1 = self.create_table_with_condition(self.dataframe1, carrier_cost_columns, self.deductable_costs_name, self.deductable_costs_columns_srt,0)
        # deductible2 = self.create_table_with_condition(self.dataframe2, carrier_cost_columns, self.deductable_costs_name, self.deductable_costs_columns_srt,0)
        #
        # nch1=self.create_table_with_condition(self.dataframe1, carrier_cost_columns, self.NCH_costs_columns_name, self.NCH_costs_columns_str,0)
        # nch2=self.create_table_with_condition(self.dataframe2, carrier_cost_columns, self.NCH_costs_columns_name, self.NCH_costs_columns_str,0)
        #
        # pc1=self.create_table_with_condition(self.dataframe1, carrier_cost_columns, self.primary_care_costs_name, self.primary_care_costs_columns_srt,0)
        # pc2=self.create_table_with_condition(self.dataframe2, carrier_cost_columns, self.primary_care_costs_name, self.primary_care_costs_columns_srt,0)
        #
        # coinsur1=self.create_table_with_condition(self.dataframe1, carrier_cost_columns, self.coinsurance_costs_name, self.coinsurance_costs_columns_srt,0)
        # coinsur2=self.create_table_with_condition(self.dataframe2, carrier_cost_columns, self.coinsurance_costs_name, self.coinsurance_costs_columns_srt,0)
        #
        # allowed1=self.create_table_with_condition(self.dataframe1, carrier_cost_columns, self.allowed_charge_costs_name, self.allowed_charge_costs_columns_srt,0)
        # allowed2=self.create_table_with_condition(self.dataframe2, carrier_cost_columns, self.allowed_charge_costs_name, self.allowed_charge_costs_columns_srt,0)


        # self.create_merged_sql(diags1, diags2, f'{self.table_name}_{self.diags_name}_TABLE')
        # self.create_merged_sql(hcpcs1, hcpcs2, f'{self.table_name}_{self.hcpcs_name}_TABLE')
        self.create_merged_sql(procs1, procs2, f'{self.table_name}_{self.procs_name}_TABLE')
        # self.create_merged_sql(deductible1, deductible2, f'{self.table_name}_{self.deductable_costs_name}_TABLE')
        # self.create_merged_sql(nch1, nch2, f'{self.table_name}_{self.NCH_costs_columns_name}_TABLE')
        # self.create_merged_sql(pc1, pc2, f'{self.table_name}_{self.primary_care_costs_name}_TABLE')
        # self.create_merged_sql(coinsur1, coinsur2, f'{self.table_name}_{self.coinsurance_costs_name}_TABLE')
        # self.create_merged_sql(allowed1, allowed2, f'{self.table_name}_{self.allowed_charge_costs_name}_TABLE')


        return None

# class MedicationsDB(BaseDB):
#     def __init__(self):
#             super().__init__()
#             self.table_name = self.metadata['datasets_data']['datasets_names']['medications']
#             self.dataframe = pd.read_csv('Data/Raw_Data/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_1.csv')
#
#     def make_sql(self):
#             self.create_sql(self.dataframe, f'{self.table_name}_TABLE')
#             return None

if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    InpatientDB().make_sql()


