import pandas as pd
from Features.base_feature import BaseFeature
from Preprocessing.Datasets import InpatientDataset, OutpatientDataset, MedicationsDataset
import json
from Features.trends import Trends
from Preprocessing.Datasets import Dataset
import re
import numpy as np
from utils.utils import merge_dfs_on_column

class MedicationFeatures(BaseFeature):
    def __init__(self):
        super().__init__()
        self.mapping_prefix = 'medication_mapping'
        self.ndc_to_rxcui_path='Data/ndc_to_rxcui.csv'
        self.ndc_to_rxcui = pd.read_csv(self.ndc_to_rxcui_path).dropna().drop_duplicates()
        with open("Data/Feature_mapping/medications.json", "r") as f:
            self.medications_dict = json.load(f)
        self.categorical_features_prefix='categorical_diagnosis'
        self.date_column_name='SRVC_DT'
        self.compliance_meds=self.medications_dict['compliance_meds']

    def calculate_batch(self, ids):

        batch_results = pd.DataFrame({self.patient_id: ids})

        medications=MedicationsDataset().get_patient_lines_in_train_time(ids)
        medications_with_type=self.merge_with_rxcui(medications, self.ndc_to_rxcui, self.medications_dict[self.mapping_prefix])
        medications_with_type_and_dosage=self.get_dosage(medications_with_type)

        counts = self.count_feature(medications_with_type_and_dosage, self.method_subgrouping_column_name)
        zero_one_features = self.get_zero_one_features(counts, self.medications_dict[self.categorical_features_prefix])
        #trends = Trends().get_trends(medications_with_type_and_dosage, self.method_subgrouping_column_name, self.patient_id,self.date_column_name)
        compliance_score=Trends().calculate_compliance(medications_with_type_and_dosage,self.date_column_name, self.compliance_meds)

        result_dfs = [batch_results, zero_one_features, compliance_score]
        batch_results = merge_dfs_on_column(result_dfs, self.patient_id)
        return batch_results

    def merge_with_rxcui(self, df, ndc_rxcui_mapping, rxcui_dict):
        rxcui_table=self.create_code_symptom_mapping(ndc_rxcui_mapping, rxcui_dict,'rxcui_description')
        relevant_df = df[df['PROD_SRVC_ID'].isin(rxcui_table.ndc.values)]
        df_with_symptom = relevant_df.merge(rxcui_table,
                                   left_on='PROD_SRVC_ID',
                                   right_on='ndc', how='left')
        return df_with_symptom

    def get_dosage(self, df):
        dosages=[(re.findall(r"[-+]?\d*\.?\d+|/d+", med)[0], self.get_dosage_unit(med)) for med in df.rxcui_description]
        df['dosage']=[dosage[0] for dosage in dosages]
        df['unit'] = [dosage[1] for dosage in dosages]
        return df

    def get_dosage_unit(self, med):
        if 'MG' in med: return 'MG'
        else: return np.nan


if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    patient_ids=Dataset().get_patient_ids()[:100]

    # ids=['00013D2EFD8E45D1','00016F745862898F','00052705243EA128','0007F12A492FD25D','000B97BA2314E971','000C7486B11E7030','00108066CA1FACCE','0011714C14B52EEB',
    #      '0011CB1FE23E91AF','00139C345A104F72','0013E139F1F37264','00157F1570C74E09']

    g=MedicationFeatures().calculate_batch(patient_ids)
    print(g)