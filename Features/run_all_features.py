import pandas as pd
from Features.base_feature import BaseFeature
from Features.demographic_features import DemographicFeatures
from Features.cost_features import CostFeatures
import json
from Features.trends import Trends
from Preprocessing.Datasets import Dataset
import sys

class RunFeatures(BaseFeature):
    def __init__(self):
        super().__init__()
        self.sample_size=self.config['experiment']['patients_number']
        self.features_dict=self.config['features']

    def create_dataset(self):
        patient_ids= Dataset().get_patient_ids()[:self.sample_size]
        all_features=pd.DataFrame({self.patient_id:patient_ids})
        for feature in self.features_dict.keys():
            if self.features_dict[feature][0]:
                feature_class=self.get_class_from_string(self.features_dict[feature][1])
                feature_df=feature_class().calculate_feature(patient_ids,self.features_dict[feature][2])
                all_features=all_features.merge(feature_df, on=self.patient_id, how='outer')
            else:
                pass
        return all_features

    def get_class_from_string(self,string):
        return getattr(sys.modules[__name__], string)

if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    g = RunFeatures().create_dataset()
    print(g)
