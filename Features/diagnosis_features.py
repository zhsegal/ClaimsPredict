import pandas as pd
from Features.base_feature import BaseFeature
from Preprocessing.Datasets import InpatientDataset
import json

class Diagnosis_features(BaseFeature):
    def __init__(self):
        super().__init__()
        self.icd9_table = pd.read_csv(self.config['preprocessing']['tables']['ICD9'])
        with open("Data/Feature_mapping/diagnosis.json", "r") as f:
            self.diags_dict = json.load(f)
        self.diags_table_col='dgns_cd'
        self.icd9_table_description_col='shortdesc'

    def calculate_batch(self, ids):
        inpat_diags=InpatientDataset('DIAG').get_patient_lines(ids)
        counts=self.count_feature(inpat_diags, self.diags_dict,self.diags_table_col)
        return counts


    def count_feature (self, df, items_dict,item_col):
        code_symptom_map = self.create_code_symptom_mapping(self.icd9_table, items_dict,self.icd9_table_description_col)
        df=df[df[self.diagnosis_column].isin(code_symptom_map.dgns_cd.values)]
        df_with_symptom=df.merge(code_symptom_map[[item_col,'item']], left_on=self.diagnosis_column, right_on=item_col, how='left')
        counts = df_with_symptom.groupby([self.patient_id, 'item']).size().unstack(fill_value=0)
        return counts

if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    ids=['00013D2EFD8E45D1','00016F745862898F','00052705243EA128','0007F12A492FD25D','000B97BA2314E971','000C7486B11E7030','00108066CA1FACCE','0011714C14B52EEB',
         '0011CB1FE23E91AF','00139C345A104F72','0013E139F1F37264','00157F1570C74E09']

    g=(Diagnosis_features().calculate_batch(ids))
    print(g)