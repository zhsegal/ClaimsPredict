import pandas as pd
from Preprocessing.Datasets import Dataset
from Preprocessing.Datasets import InpatientDataset, OutpatientDataset,CarrierDataset, MedicationsDataset
from Features.base_feature import BaseFeature
import json

class BaseTarget(BaseFeature):
    def __init__(self):
        super().__init__()
        with open("Data/Targets/targets.json", "r") as f:
            self.all_targets = json.load(f)
        self.date_columns='CLM_FROM_DT'
        self.mapping_prefix = 'medication_mapping'

        with open("Data/Feature_mapping/medications.json", "r") as f:
            self.medications_dict = json.load(f)
        pass





    def get_all_diagnosis_in_post_train_time(self, ids):
        relevant_cols=self.relevant_methods_columns(self.diags_name)
        inpat_diags=InpatientDataset(self.diags_name).get_all_diagnosis_in_post_train_time(ids)[relevant_cols]
        outpat_diags=OutpatientDataset(self.diags_name).get_all_diagnosis_in_post_train_time(ids)[relevant_cols]
        carrier_diags=CarrierDataset(self.diags_name).get_all_diagnosis_in_post_train_time(ids)[relevant_cols]


        return pd.concat([inpat_diags,outpat_diags,carrier_diags])

    def get_all_procedures_in_post_train_time(self, ids):
        relevant_cols=self.relevant_methods_columns(self.procs_name)
        inpat_diags=InpatientDataset(self.procs_name).get_all_diagnosis_in_post_train_time(ids)[relevant_cols]
        outpat_procs=OutpatientDataset(self.procs_name).get_all_diagnosis_in_post_train_time(ids)[relevant_cols]
        carrier_procs=CarrierDataset(self.procs_name).get_all_diagnosis_in_post_train_time(ids)[relevant_cols]


        return pd.concat([inpat_diags, outpat_procs, carrier_procs])


    def get_all_medications_in_post_train_time(self, ids):
        #relevant_cols=self.relevant_methods_columns(self.procs_name)
        medications=MedicationsDataset().get_all_diagnosis_in_post_train_time(ids)
        medications_with_type=self.merge_with_rxcui(medications, self.ndc_to_rxcui, self.medications_dict[self.mapping_prefix])

        return medications_with_type


    def relevant_methods_columns(self, method_name):
        return [self.patient_id,f'{method_name}_CODE', self.date_columns]