import multiprocessing as mp
from config.configuration import Configuration
import pandas as pd
from datetime import datetime

class BaseFeature():
    def __init__(self):
        self.config=Configuration().get_config()
        self.batch_size=self.config['multiprocessing']['batch_size']
        self.diagnosis_prefix=self.config['preprocessing']['method_names']['diagnosis']
        self.diagnosis_column=f'{self.diagnosis_prefix}_CODE'
        self.patient_id='DESYNPUF_ID'
        self.icd9_table_description_col = 'shortdesc'
        self.icd9_table = pd.read_csv(self.config['preprocessing']['tables']['ICD9'])
        self.item_col_name='item'
        self.diag_features_prefix='DIAG'
        self.train_end_time=datetime.strptime(self.config['experiment']['experiment'], '%Y:%m:%d')

    def calculate_feature(self, ids):

        added_features = self.run_multiprocess(ids)
        return added_features

    def run_multiprocess(self, ids):
        results_df = pd.DataFrame()
        pool=mp.Pool(processes=self.config['multiprocessing']['process_number'])

        results=[]

        for ids in (self.chunkizer(ids,self.batch_size)):
            result=pool.apply_async(func=self.calculate_batch, args=(ids,))
            results.append(result)

        pool.close()
        pool.join()
        for result in results: results_df = results_df.append(result.get())
        return results_df


    def chunkizer(self, ids,size):
        for i in range(0, len(ids), size):
            yield ids[i:i + size]


    def create_code_symptom_mapping (self,table, item_dict, item_description_columns):
        mapping=pd.DataFrame()
        for key in item_dict:
            values='|'.join(item_dict.get(key))
            item_df=table[table[item_description_columns].str.contains(values)]
            item_df['item']=key
            mapping=mapping.append(item_df)
        return mapping

    def get_zero_one_features(self, count_data, cat_features):
        count_data[count_data[cat_features] > 0] = 1
        return count_data

    def merge_with_symptom_name(self, df, items_dict, feature_col_name):
        code_symptom_map = self.create_code_symptom_mapping(self.icd9_table, items_dict,
                                                            self.icd9_table_description_col)
        df = df[df[self.diagnosis_column].isin(code_symptom_map.dgns_cd.values)]
        df_with_symptom = df.merge(code_symptom_map[[feature_col_name, self.item_col_name]], left_on=self.diagnosis_column,
                                   right_on=feature_col_name, how='left')
        return df_with_symptom

    def count_feature (self, df):

        counts = df.groupby([self.patient_id, self.item_col_name]).size().unstack(fill_value=0)
        return counts



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
