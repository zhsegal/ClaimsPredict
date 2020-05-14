import pandas as pd
from Features.base_feature import BaseFeature
from Preprocessing.Datasets import InpatientDataset, OutpatientDataset,CarrierDataset
import json
from Features.trends import Trends
from Preprocessing.Datasets import Dataset
from utils.utils import string_to_int_else_nan
from utils.utils import merge_dfs_on_column


class ProceduresFeatures(BaseFeature):
    def __init__(self):
        super().__init__()
        self.mapping_prefix = 'procedure_mapping'
        self.date_column_name = 'CLM_FROM_DT'
        self.method_name_columns = 'PROCS_CODE'
        #self.categorical_features_prefix = 'categorical_diagnosis'
        with open("Data/Feature_mapping/procedures.json", "r") as f:
            self.procs_dict = json.load(f)

    def calculate_batch(self, ids):
        batch_results = pd.DataFrame({self.patient_id: ids})

        inpat_procs = InpatientDataset(self.procs_name).get_patient_lines_in_train_time(ids)
        inpat_procs.PROCS_CODE=string_to_int_else_nan(inpat_procs.PROCS_CODE)
        inpat_procs_with_symptom_name=self.merge_proc_with_symptom_name(inpat_procs, self.procs_dict[self.mapping_prefix], self.method_name_columns)

        outpat_procs = OutpatientDataset(self.procs_name).get_patient_lines_in_train_time(ids)
        outpat_procs.PROCS_CODE = string_to_int_else_nan(outpat_procs.PROCS_CODE)
        outpat_procs_with_symptom_name = self.merge_proc_with_symptom_name(outpat_procs,
                                                                          self.procs_dict[self.mapping_prefix],
                                                                          self.method_name_columns)

        carrier_procs = CarrierDataset(self.procs_name).get_patient_lines_in_train_time(ids)
        carrier_procs.PROCS_CODE = string_to_int_else_nan(outpat_procs.PROCS_CODE)
        carrier_procs_with_symptom_name = self.merge_proc_with_symptom_name(carrier_procs,
                                                                           self.procs_dict[self.mapping_prefix],
                                                                           self.method_name_columns)

        counts = self.count_feature(inpat_procs_with_symptom_name, self.method_subgrouping_column_name)
        #zero_one_features = self.get_zero_one_features(counts, self.procs_dict[self.categorical_features_prefix])
        trends = Trends().get_trends(outpat_procs_with_symptom_name, self.method_subgrouping_column_name, self.patient_id,
                                     self.date_column_name)

        result_dfs = [batch_results, counts, trends]
        batch_results = merge_dfs_on_column(result_dfs, self.patient_id)
        return batch_results

if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    patient_ids=Dataset().get_patient_ids()[:500]

    ids=['00013D2EFD8E45D1','00016F745862898F','00052705243EA128','0007F12A492FD25D','000B97BA2314E971','000C7486B11E7030','00108066CA1FACCE','0011714C14B52EEB',
         '0011CB1FE23E91AF','00139C345A104F72','0013E139F1F37264','00157F1570C74E09']

    g=ProceduresFeatures().calculate_batch(patient_ids)
    print(g)