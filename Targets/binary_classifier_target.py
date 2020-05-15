import pandas as pd
from Preprocessing.Datasets import Dataset
from Targets.base_target import BaseTarget
import json

class CategoricalTarget(BaseTarget):
    def __init__(self,target):
        super().__init__()

        self.target_dict=self.all_targets[target]
        self.target_methods = self.target_dict['methods']
        self.diagnosis_dict=self.target_dict['target_specs']['target_diagnosis']
        self.method_name_columns = 'dgns_cd'


    def calculate_batch(self, ids):
        batch_results=pd.DataFrame({self.patient_id:ids})
        if 'diags' in self.target_methods:
            diagnosis=self.get_all_diagnosis_in_post_train_time(ids)
            diagnosis_with_target_name=self.merge_with_symptom_name(diagnosis, self.diagnosis_dict, self.method_name_columns)
            first_diagnosis_time = diagnosis_with_target_name.groupby(self.patient_id)[self.date_columns].first().reset_index(name='diagnosis_time')
            first_diagnosis_time['target']=1
            batch_results=batch_results.merge(first_diagnosis_time, on=self.patient_id, how='left')

        #preocedures=self.get_all_procedures_in_post_train_time(patient_ids) if 'procs' in self.target_methods else None
        #medications=self.get_all_medications_in_post_train_time(patient_ids) if 'medications' in self.target_methods else None
        batch_results['diagnosis_time']=batch_results['diagnosis_time'].fillna(pd.to_datetime('2010-12-31'))

        return batch_results.fillna(0)

if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    patient_ids=Dataset().get_patient_ids()[:500]

    ids=['00013D2EFD8E45D1','00016F745862898F','00052705243EA128','0007F12A492FD25D','000B97BA2314E971','000C7486B11E7030','00108066CA1FACCE','0011714C14B52EEB',
         '0011CB1FE23E91AF','00139C345A104F72','0013E139F1F37264','00157F1570C74E09']

    g=CategoricalTarget('fibromyalgia').calculate_batch(patient_ids)
    print(g)