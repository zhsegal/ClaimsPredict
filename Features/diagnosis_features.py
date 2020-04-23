import pandas as pd
from Features.base_feature import BaseFeature
from Preprocessing.Datasets import InpatientDataset

class Diagnosis_features(BaseFeature):
    def __init__(self):
        super().__init__()
        pass

    def calculate_batch(self, ids):
        inpat_diags=InpatientDataset('DIAG').get_patient_lines(ids)

        return inpat_diags



if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    ids=['00013D2EFD8E45D1','00016F745862898F','00052705243EA128','0007F12A492FD25D','000B97BA2314E971','000C7486B11E7030','00108066CA1FACCE','0011714C14B52EEB',
         '0011CB1FE23E91AF','00139C345A104F72','0013E139F1F37264','00157F1570C74E09']

    g=(Diagnosis_features().calculate_feature(ids))
    print(g)