import multiprocessing as mp
from config.configuration import Configuration
import pandas as pd
from datetime import datetime
import time
from tqdm import tqdm
from utils.utils import time_from_sting,string_to_int_else_nan
import json
import os.path
import pathlib

class BaseFeature():
    def __init__(self):

        self.config=Configuration().get_config()
        self.random_state = self.config['experiment']['random_state']
        #self.batch_size=self.config['multiprocessing']['batch_size']
        self.batch_size=self.config['multiprocessing']['batch_size']
        self.method_subgrouping_column_name='Group'

        self.diagnosis_prefix=self.config['preprocessing']['method_names']['diagnosis']
        self.diagnosis_column=f'{self.diagnosis_prefix}_CODE'
        self.patient_id='DESYNPUF_ID'
        self.icd9_table_description_col = 'shortdesc'
        self.icd9_table = pd.read_csv(self.config['preprocessing']['tables']['ICD9'])
        self.icd_proc_table=pd.read_csv(self.config['preprocessing']['tables']['ICD9_PROC'])
        self.item_col_name='item'

        self.train_end_time=time_from_sting((self.config['experiment']['experiment']))
        self.diags_name = self.config['preprocessing']['method_names']['diagnosis']
        self.procs_name = self.config['preprocessing']['method_names']['procedure']
        self.hcpcs_name = self.config['preprocessing']['method_names']['hcpcs']
        self.hospitalizations_name = self.config['preprocessing']['method_names']['hospitalizations']
        with open("config\datasets_metadata.json", "r") as f:
            self.metadata = json.load(f)
        self.features_cache_path= 'cache/features'
        self.targets_cache_path='cache/targets'
        self.ndc_to_rxcui_path = 'Data/ndc_to_rxcui.csv'
        self.ndc_to_rxcui = pd.read_csv(self.ndc_to_rxcui_path, low_memory=False).dropna().drop_duplicates()

    def get_cache_path(self, ids, feature_name, path):
        return f'{path}/{feature_name}_{len(ids)}_patients.csv'



    def calculate_feature(self, ids, feature_name):
        path=self.get_cache_path(ids, feature_name,self.features_cache_path)
        if os.path.isfile(path):
            print (f'{feature_name} feature exists, loading cache')
            return pd.read_csv(path)

        else:
            print(f'{feature_name} feature doesnt exists, calculating')
            calculated_featrue = self.run_multiprocess(ids)
            calculated_featrue.to_csv(path, index=False)
            print(f'{feature_name} feature calculated and cached')
            return calculated_featrue

    def calculate_target(self, ids, target_name):
        path=self.get_cache_path(ids, target_name,self.targets_cache_path)
        if os.path.isfile(path):
            print (f'target {target_name} exists, loading cache')
            return pd.read_csv(path)

        else:
            print(f'target {target_name} doesnt exists, calculating')
            calculated_featrue = self.run_multiprocess(ids)
            calculated_featrue.to_csv(path, index=False)
            print(f'target {target_name} calculated and cached')
            return calculated_featrue


    def run_multiprocess(self, patient_ids):
        results_df = pd.DataFrame()
        results = []
        batch_number = len(range(0, len(patient_ids), self.batch_size))
        with mp.Pool(processes=self.config['multiprocessing']['process_number']) as p:

            r=list(tqdm(p.imap(self.calculate_batch, iterable=self.chunkizer(patient_ids, self.batch_size)),total=batch_number))
            p.close()
            p.join()

        for result in r: results_df = results_df.append(result, ignore_index=True)
        return results_df




    def chunkizer(self, ids,size):
        for i in range(0, len(ids), size):
            yield ids[i:i + size]


    def create_code_symptom_mapping (self,table, item_dict, item_description_columns):
        mapping=pd.DataFrame()
        for key in item_dict:
            values='|'.join(item_dict.get(key))
            item_df=table[table[item_description_columns].str.contains(values)]
            item_df[self.method_subgrouping_column_name]=key
            mapping=mapping.append(item_df)
        return mapping

    def get_zero_one_features(self, count_data, cat_features):
        count_data[count_data[cat_features] > 0] = 1
        return count_data

    def merge_with_symptom_name(self, df, items_dict, feature_col_name):
        code_symptom_map = self.create_code_symptom_mapping(self.icd9_table, items_dict,
                                                            self.icd9_table_description_col)
        df = df[df[self.diagnosis_column].isin(code_symptom_map.dgns_cd.values)]
        df_with_symptom = df.merge(code_symptom_map[[feature_col_name, self.method_subgrouping_column_name]], left_on=self.diagnosis_column,
                                   right_on=feature_col_name, how='left')
        return df_with_symptom

    def merge_proc_with_symptom_name(self, df, items_dict, feature_col_name):
        code_symptom_map = self.create_code_symptom_mapping(self.icd_proc_table, items_dict,
                                                            self.icd9_table_description_col)
        df = df[df['PROCS_CODE'].isin(code_symptom_map.prcdrcd.values)]
        df_with_symptom = df.merge(code_symptom_map[['prcdrcd', self.method_subgrouping_column_name]], left_on='PROCS_CODE',
                                   right_on='prcdrcd', how='left')
        return df_with_symptom

    def count_feature (self, df, count_column):

        counts = df.groupby([self.patient_id, count_column]).size().unstack(fill_value=0)
        return counts

    def events_counter(self, df, count_on_columns):
        return df.groupby(self.patient_id)[count_on_columns].count()

    def unique_event_counter(self, df, count_on_columns):
        return df.groupby(self.patient_id)[count_on_columns].nunique()

    def merge_with_rxcui(self, df, ndc_rxcui_mapping, rxcui_dict):
        rxcui_table=self.create_code_symptom_mapping(ndc_rxcui_mapping, rxcui_dict,'rxcui_description')
        relevant_df = df[df['PROD_SRVC_ID'].isin(rxcui_table.ndc.values)]
        df_with_symptom = relevant_df.merge(rxcui_table,
                                   left_on='PROD_SRVC_ID',
                                   right_on='ndc', how='left')
        return df_with_symptom

    def calculate_batch(self, ids):
        raise NotImplemented




if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    ids=['00013D2EFD8E45D1','00016F745862898F','00052705243EA128','0007F12A492FD25D','000B97BA2314E971','000C7486B11E7030','00108066CA1FACCE','0011714C14B52EEB',
         '0011CB1FE23E91AF','00139C345A104F72','0013E139F1F37264','00157F1570C74E09']

    BaseFeature().calculate_feature(ids)

    #, callback = collect_result
    # def collect_result(result):
    #         nonlocal results
    #         results = results.append(result)
    #         print(result)
