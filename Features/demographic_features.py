import pandas as pd
from Features.base_feature import BaseFeature
from Preprocessing.Datasets import BeneficiaryDataset
import json
from Features.trends import Trends
from Preprocessing.Datasets import Dataset
from utils.utils import parse_date_column

class DemographicFeatures(BaseFeature):
    def __init__(self):
        super().__init__()
        self.year_08_str='08'

    def calculate_batch(self, ids):
        results = BeneficiaryDataset(self.year_08_str).get_patient_demographics_lines(ids)
        results['AGE']=[(self.train_end_time - date).days / 365 for date in results.BENE_BIRTH_DT]
        return None




if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    patient_ids=Dataset().get_patient_ids()

    ids=['00013D2EFD8E45D1','00016F745862898F','00052705243EA128','0007F12A492FD25D','000B97BA2314E971','000C7486B11E7030','00108066CA1FACCE','0011714C14B52EEB',
         '0011CB1FE23E91AF','00139C345A104F72','0013E139F1F37264','00157F1570C74E09']

    g=(DemographicFeatures().calculate_batch(ids))
    print(g)