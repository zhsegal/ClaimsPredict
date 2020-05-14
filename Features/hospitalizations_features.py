import pandas as pd
from Features.base_feature import BaseFeature
from Preprocessing.Datasets import InpatientDataset, OutpatientDataset
import json
from Features.trends import Trends
from Preprocessing.Datasets import Dataset
from utils.utils import parse_date_column, merge_dfs_on_column

class HospitalizationFeatures(BaseFeature):
    def __init__(self):
        super().__init__()
        self.event_columns='CLM_ID'
        self.drg_column='CLM_DRG_CD'
        self.hospital_id='PRVDR_NUM'

    def calculate_batch(self, ids):
        batch_results = pd.DataFrame({self.patient_id: ids})

        hospitalizations=InpatientDataset(self.hospitalizations_name).get_patient_lines_in_train_time(ids)

        hospitalization_events=self.hospiatlization_durations(hospitalizations)
        drg_counts=self.unique_event_counter(hospitalizations,self.drg_column)
        hospitals_counts=self.unique_event_counter(hospitalizations,self.hospital_id)

        result_dfs = [batch_results, hospitalization_events, drg_counts,hospitals_counts]
        batch_results = merge_dfs_on_column(result_dfs, self.patient_id)
        return batch_results

    def hospiatlization_durations(self, hosp):
        hosp=parse_date_column(hosp,'NCH_BENE_DSCHRG_DT')
        hosp['duration']=(hosp['NCH_BENE_DSCHRG_DT']-hosp['CLM_ADMSN_DT']).dt.days
        results=hosp.groupby(self.patient_id).agg({'duration':['mean', 'sum','count']})
        return results.droplevel(0,axis=1)

    def drg_counts(self, ids):
        pass


if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    patient_ids=Dataset().get_patient_ids()

    # #ids=['00013D2EFD8E45D1','00016F745862898F','00052705243EA128','0007F12A492FD25D','000B97BA2314E971','000C7486B11E7030','00108066CA1FACCE','0011714C14B52EEB',
    #      '0011CB1FE23E91AF','00139C345A104F72','0013E139F1F37264','00157F1570C74E09']

    g=HospitalizationFeatures().calculate_batch(patient_ids)
    print(g)