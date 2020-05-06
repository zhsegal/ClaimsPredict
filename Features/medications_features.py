import pandas as pd
from Features.base_feature import BaseFeature
from Preprocessing.Datasets import InpatientDataset, OutpatientDataset, MedicationsDataset
import json
from Features.trends import Trends
from Preprocessing.Datasets import Dataset

class MedicationFeatures(BaseFeature):
    def __init__(self):
        super().__init__()
        self.mapping_prefix = 'medication_mapping'
        self.ndc_to_rxcui_path='Data/ndc_to_rxcui.csv'
        self.ndc_to_rxcui = pd.read_csv(self.ndc_to_rxcui_path).dropna()
        with open("Data/Feature_mapping/medications.json", "r") as f:
            self.medications_dict = json.load(f)

    def calculate_batch(self, ids):

        medications=MedicationsDataset().get_patient_lines_in_train_time(ids)
        mappint=self.get_ndc_from_rxcui(self.ndc_to_rxcui,self.medications_dict[self.mapping_prefix])


        return None

    def get_ndc_from_rxcui(self, ndc_rxcui_mapping, rxcui_dict):
        return self.create_code_symptom_mapping(ndc_rxcui_mapping, rxcui_dict,'rxcui_description')


if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    #patient_ids=Dataset().get_patient_ids()

    ids=['00013D2EFD8E45D1','00016F745862898F','00052705243EA128','0007F12A492FD25D','000B97BA2314E971','000C7486B11E7030','00108066CA1FACCE','0011714C14B52EEB',
         '0011CB1FE23E91AF','00139C345A104F72','0013E139F1F37264','00157F1570C74E09']

    g=MedicationFeatures().calculate_batch(ids)
    print(g)