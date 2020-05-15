import pandas as pd
from Features.base_feature import BaseFeature
from Features.demographic_features import DemographicFeatures
from Features.cost_features import CostFeatures
from Features.diagnosis_features import DiagnosisFeatures
from Features.procedures_features import ProceduresFeatures
from Features.hospitalizations_features import HospitalizationFeatures
from Features.medications_features import MedicationFeatures
from Preprocessing.Datasets import Dataset
import sys
import os

class RunFeatures(BaseFeature):
    def __init__(self):
        super().__init__()
        self.sample_size=self.config['experiment']['patients_number']
        self.features_dict=self.config['features']
        self.all_features_suffix='all_features'

    def get_features(self):
        patient_ids = Dataset().get_patient_ids()[:self.sample_size]
        path=self.get_cache_path(patient_ids,self.all_features_suffix,self.features_cache_path )
        if os.path.isfile(path):
            print ('all features data frame exists, loading cache')
            return pd.read_csv(path)
        else:
            print('allfeatures doesnt exists, calculating')
            all_features = self.create_dataset(patient_ids)
            all_features.to_csv(path, index=False)
            print('all feature calculated and cached')
            return all_features


    def create_dataset(self, patient_ids):

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

    g = RunFeatures().get_features()
    print(g)
