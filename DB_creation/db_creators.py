import pandas as pd
import sqlite3
import json
from config.configuration import Configuration
from utils.utils import get_rxcui_from_ndc,merge_dfs_on_column
import os

class BaseDB:


    def __init__(self):
        os.chdir('C:\\Users\\Boston\\PycharmProjects\\ClaimstoModels\\')
        with open("config\datasets_metadata.json", "r") as f:
            self.metadata = json.load(f)

        self.config = Configuration().get_config()

        self.diag_columns_str=['ICD9_DGNS']
        self.proc_columns_str=['ICD9_PRCDR']
        self.hcpcs_columns_str=['HCPCS']

        self.outpatient_db_title=self.get_db_name('outpatient')
        self.medications_db_title = self.get_db_name('medications')
        self.beneficifary_db_title = self.get_db_name('beneficiary')
        self.inpatient_db_title = self.get_db_name('inpatient')
        self.carrier_db_title = self.get_db_name('carrier')

        self.medicaion_db_name=f'{self.medications_db_title}_TABLE'
        self.beneficiary_db_name_08=f'{self.beneficifary_db_title}_08_TABLE'
        self.beneficiary_db_name_09 =f'{self.beneficifary_db_title}_09_TABLE'
        self.beneficiary_db_name_10 =f'{self.beneficifary_db_title}_10_TABLE'


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


    def db_name_to_path(self, db_name, db_folder='DB'):
        return f'{db_folder}/{db_name}.db'

    def get_db_name(self, db_name):
        return self.metadata['datasets_data']['datasets_names'][db_name]

    def get_method_name(self, method_name):
        return self.config['preprocessing']['method_names'][method_name]

    def get_method_db(self, table, columns,method_name,methods_col_strings, db_table_name):
        table = self.create_table(table, columns,  method_name, methods_col_strings)
        self.create_sql(table, f'{db_table_name}_{method_name}_TABLE')
        print (f'{db_table_name}_{method_name}_TABLE created')

    def get_db_table_name(self, table, method):
        return f'{table}_{method}_TABLE'

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

    def make_sql(self, method):
        oupatient_diagnosis_columns = self.metadata['raw_tables_columns']['outpatient_diagnosis_columns']
        oupatient_costs_columns = self.metadata['raw_tables_columns']['outpatient_cost_columns']

        name = self.get_method_name(method)

        if method=='diagnosis':
            column_str=self.diag_columns_str
            self.get_method_db(self.dataframe, oupatient_diagnosis_columns, name, column_str,self.outpatient_db_title)
        elif method=='procedure':
            column_str = self.proc_columns_str
            self.get_method_db(self.dataframe, oupatient_diagnosis_columns, name, column_str,self.outpatient_db_title)
        elif method=='hcpcs':
            column_str = self.hcpcs_columns_str
            self.get_method_db(self.dataframe, oupatient_diagnosis_columns, name, column_str,self.outpatient_db_title)
        elif method == 'costs':
            costs = self.dataframe[oupatient_costs_columns]
            self.create_sql(costs, self.get_db_table_name(self.outpatient_db_title, name))
        else:
            raise SystemExit('must be either diags procs or hscpcs')



class InpatientDB(BaseDB):
    def __init__(self):
        super().__init__()
        self.dataframe=pd.read_csv('Data/Raw_Data/DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.csv')
        self.table_name = self.metadata['datasets_data']['datasets_names']['inpatient']
        self.date_column='CLM_FROM_DT'

    def make_sql(self,method):
        inpatient_diagnosis_columns = self.metadata['raw_tables_columns']['inpatient_diagnosis_columns']
        inpatient_hospitalization_columns = self.metadata['raw_tables_columns']['inpatient_hospitalization_columns']
        inpatient_costs_columns = self.metadata['raw_tables_columns']['inpatient_cost_columns']


        name = self.get_method_name(method)

        if method=='diagnosis':
            column_str=self.diag_columns_str
            self.get_method_db(self.dataframe, inpatient_diagnosis_columns, name, column_str, self.inpatient_db_title)
        elif method=='procedure':
            column_str = self.proc_columns_str
            self.get_method_db(self.dataframe, inpatient_diagnosis_columns, name, column_str,self.inpatient_db_title)
        elif method=='hcpcs':
            column_str = self.hcpcs_columns_str
            self.get_method_db(self.dataframe, inpatient_diagnosis_columns, name, column_str,self.inpatient_db_title)
        elif method == 'costs':
            costs = self.dataframe[inpatient_costs_columns].dropna(subset=[self.date_column]).fillna(0)
            self.create_sql(costs, self.get_db_table_name(self.inpatient_db_title, name))
        elif method=='hospitalizations':
            hospitalizations = self.dataframe[inpatient_hospitalization_columns]
            self.create_sql(hospitalizations, self.get_db_table_name(self.inpatient_db_title, name))
        else:
            raise SystemExit('must be either diags procs or hscpcs')



class CarrierDB(BaseDB):
    def __init__(self):
        super().__init__()
        self.dataframe1=pd.read_csv('Data/Raw_Data/DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.csv')
        self.dataframe2 =pd.read_csv('Data/Raw_Data/DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.csv')
        self.dataframe=pd.concat([self.dataframe1,self.dataframe2])
        self.NCH_costs_columns_str = ['NCH_PMT']
        self.deductable_costs_columns_srt = ['DDCTBL_AMT']
        self.primary_care_costs_columns_srt = ['PRMRY_PYR_PD_AMT']
        self.coinsurance_costs_columns_srt = ['COINSRNC_AMT']
        self.allowed_charge_costs_columns_srt = ['ALOWD_CHRG']

    def make_sql(self, method):
        carrier_method_columns = self.metadata['raw_tables_columns']['carrier_method_columns']
        carrier_cost_columns = self.metadata['raw_tables_columns']['carrier_cost_columns']
        name=self.get_method_name(method)

        if method=='diagnosis':
            column_str=self.diag_columns_str
            self.get_method_db(self.dataframe, carrier_method_columns, name, column_str, self.carrier_db_title)
        elif method=='procedure':
            column_str = self.proc_columns_str
            self.get_method_db(self.dataframe, carrier_method_columns, name, column_str,self.carrier_db_title)
        elif method=='hcpcs':
            column_str = self.hcpcs_columns_str
            self.get_method_db(self.dataframe, carrier_method_columns, name, column_str,self.carrier_db_title)
        elif method=='deductible_costs':
            deductible = self.create_table_with_condition(self.dataframe, carrier_cost_columns,
                                                           name,
                                                           self.deductable_costs_columns_srt, 0)
            self.create_sql(deductible,self.get_db_table_name(self.carrier_db_title, name))

        elif method == 'NCH_costs':
            NCH = self.create_table_with_condition(self.dataframe, carrier_cost_columns,
                                                          name,
                                                          self.NCH_costs_columns_str, 0)
            self.create_sql(NCH, self.get_db_table_name(self.carrier_db_title, name))

        elif method == 'primary_care_costs':
            PC = self.create_table_with_condition(self.dataframe, carrier_cost_columns,
                                                          name,
                                                          self.primary_care_costs_columns_srt, 0)
            self.create_sql(PC, self.get_db_table_name(self.carrier_db_title, name))

        elif method == 'coinsurance_costs':
            coinsur = self.create_table_with_condition(self.dataframe, carrier_cost_columns,
                                                          name,
                                                          self.coinsurance_costs_columns_srt, 0)
            self.create_sql(coinsur, self.get_db_table_name(self.carrier_db_title, name))

        elif method == 'allowd_chargae':
            allowed = self.create_table_with_condition(self.dataframe, carrier_cost_columns,
                                                          name,
                                                          self.allowed_charge_costs_columns_srt, 0)
            self.create_sql(allowed, self.get_db_table_name(self.carrier_db_title, name))

        else:
            raise SystemExit('must be either diags procs or hscpcs')




