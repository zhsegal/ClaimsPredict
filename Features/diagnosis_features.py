import pandas as pd
from Features.base_feature import BaseFeature
from Preprocessing.Datasets import InpatientDataset, OutpatientDataset
import json
from Features.trends import Trends
from Preprocessing.Datasets import Dataset

class DiagnosisFeatures(BaseFeature):
    def __init__(self):
        super().__init__()

        with open("Data/Feature_mapping/diagnosis.json", "r") as f:
            self.diags_dict = json.load(f)
        self.mapping_prefix='diagnosis_mapping'
        self.method_name_columns= 'dgns_cd'
        self.categorical_features_prefix='categorical_diagnosis'
        self.date_column_name='CLM_FROM_DT'

    def calculate_batch(self, ids):


        inpat_diags=InpatientDataset(self.diags_name).get_patient_lines_in_train_time(ids)
        inpat_diags_with_symptom_name=self.merge_with_symptom_name(inpat_diags, self.diags_dict[self.mapping_prefix], self.method_name_columns)

        outpat_diags=OutpatientDataset(self.diags_name).get_patient_lines_in_train_time(ids)
        outpat_diags_with_symptom_name=self.merge_with_symptom_name(outpat_diags, self.diags_dict[self.mapping_prefix], self.method_name_columns)

        counts=self.count_feature(inpat_diags_with_symptom_name,self.item_col_name)
        zero_one_features=self.get_zero_one_features(counts, self.diags_dict[self.categorical_features_prefix])
        trends=Trends().get_trends(outpat_diags_with_symptom_name, self.diags_dict[self.mapping_prefix], self.item_col_name, self.patient_id, self.date_column_name)
        return counts






if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    patient_ids=Dataset().get_patient_ids()

    ids=['00013D2EFD8E45D1','00016F745862898F','00052705243EA128','0007F12A492FD25D','000B97BA2314E971','000C7486B11E7030','00108066CA1FACCE','0011714C14B52EEB',
         '0011CB1FE23E91AF','00139C345A104F72','0013E139F1F37264','00157F1570C74E09']

    g=(DiagnosisFeatures().calculate_batch(ids))
    print(g)