import pandas as pd
from Preprocessing.Datasets import Dataset
from Targets.base_target import BaseTarget

class CategoricalTarget(BaseTarget):
    def __init__(self):
        super().__init__()
        self.target_dict=self.config['targets']['fibromyalgia']
        pass

    def calculate_batch(self, ids):
        #diagnosis=self.get_all_diagnosis_in_post_train_time(patient_ids) if 'diags' in self.target_dict else None
        #preocedures=self.get_all_procedures_in_post_train_time(patient_ids) if 'procs' in self.target_dict else None
        medications=self.get_all_medications_in_post_train_time(patient_ids) if 'medications' in self.target_dict else None


        return medications

if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    patient_ids=Dataset().get_patient_ids()[:500]

    ids=['00013D2EFD8E45D1','00016F745862898F','00052705243EA128','0007F12A492FD25D','000B97BA2314E971','000C7486B11E7030','00108066CA1FACCE','0011714C14B52EEB',
         '0011CB1FE23E91AF','00139C345A104F72','0013E139F1F37264','00157F1570C74E09']

    g=CategoricalTarget().calculate_batch(patient_ids)
    print(g)