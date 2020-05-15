import pandas as pd
from Features.base_feature import BaseFeature
from Preprocessing.Datasets import Dataset
from Targets.binary_classifier_target import CategoricalTarget
import sys
import os

class RunTargets(BaseFeature):
    def __init__(self):
        super().__init__()
        self.sample_size = self.config['experiment']['patients_number']
        self.target_list=self.config['targets'].keys()
        self.cache_path='cache/targets'

    def get_targets(self):

        patient_ids = Dataset().get_patient_ids()[:self.sample_size]
        for target in list(self.target_list):
            target_df=CategoricalTarget(target).calculate_target(patient_ids, target)
            return target_df



if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    g = RunTargets().get_targets()
    print(g)
