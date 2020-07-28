import pandas as pd
from Features.base_feature import BaseFeature
from Preprocessing.Datasets import InpatientDataset, OutpatientDataset, CarrierDataset,BeneficiaryDataset
import json
from Features.trends import Trends
from Preprocessing.Datasets import Dataset
from utils.utils import parse_date_column, merge_dfs_on_column
import time

class CostFeatures(BaseFeature):
    def __init__(self):
        super().__init__()
        self.deductable_costs_name = self.config['preprocessing']['method_names']['deductible_costs']
        self.NCH_costs_columns_name = self.config['preprocessing']['method_names']['NCH_costs']
        self.primary_care_costs_name = self.config['preprocessing']['method_names']['primary_care_costs']
        self.coinsurance_costs_name = self.config['preprocessing']['method_names']['coinsurance_costs']
        self.allowed_charge_costs_name = self.config['preprocessing']['method_names']['allowd_chargae']
        self.inpatient_costs_name = self.config['preprocessing']['method_names']['inpatient_costs']
        self.outpatient_costs_name = self.config['preprocessing']['method_names']['outpatient_costs']
        self.year_08_str = '08'
        self.cost_col_string = 'AM'

    def calculate_batch(self, ids):
        results = pd.DataFrame({self.patient_id: ids})
        # carrier_costs=self.get_carrier_costs(ids)
        # inpatient_costs=self.get_inpatient_costs(ids)
        # outpatient_costs=self.get_outpatient_costs(ids)
        # complete_costs=  merge_dfs_on_column([results,carrier_costs,inpatient_costs,outpatient_costs], self.patient_id)
        time.sleep(3)
        return results

        # todo add medication costs

    def get_carrier_costs(self, ids):
        results = pd.DataFrame({self.patient_id: ids})
        carrier_primary_care_costs = CarrierDataset(self.primary_care_costs_name).get_patient_lines_in_train_time(ids)
        carrier_nch_costs = CarrierDataset(self.NCH_costs_columns_name).get_patient_lines_in_train_time(ids)
        carrier_deductable_costs = CarrierDataset(self.deductable_costs_name).get_patient_lines_in_train_time(ids)
        carrier_coinsurance_costs = CarrierDataset(self.coinsurance_costs_name).get_patient_lines_in_train_time(ids)
        carrier_allowed_charge_costs = CarrierDataset(self.allowed_charge_costs_name).get_patient_lines_in_train_time(ids)

        carrier_dfs = [carrier_primary_care_costs, carrier_nch_costs, carrier_deductable_costs,
                       carrier_coinsurance_costs,
                       carrier_allowed_charge_costs]

        carrier_cost_sum = self.sum_by_patients_for_dfs(results, carrier_dfs)

        return carrier_cost_sum


    def sum_by_patients_for_dfs(self, base_df, dfs):
        for df in dfs:
            cost_col = [col for col in df.columns if col.startswith('COSTS')][0]
            temp_result = df.groupby(self.patient_id)[cost_col].sum().reset_index(name=cost_col.replace('_CODE', ''))
            base_df = base_df.merge(temp_result, on=self.patient_id, how='outer')

        return base_df

    def get_inpatient_costs(self, ids):
        inpatient_costs = InpatientDataset('costs').get_patient_lines_in_train_time(ids)
        inpatient_columns = [col for col in inpatient_costs.columns if self.cost_col_string in col]
        inpatient_trends = Trends().get_sum_trends(inpatient_costs, inpatient_columns, 'CLM_FROM_DT')
        inpatient_cost_sum = inpatient_costs.groupby(self.patient_id)[inpatient_columns].sum().reset_index()
        return pd.merge(inpatient_trends,inpatient_cost_sum, how='outer',on=self.patient_id )

    def get_outpatient_costs(self, ids):
        outpatient_costs = OutpatientDataset('costs').get_patient_lines_in_train_time(ids)
        outpatient_columns = [col for col in outpatient_costs.columns if self.cost_col_string in col]
        outpatient_trends = Trends().get_sum_trends(outpatient_costs, outpatient_columns, 'CLM_FROM_DT')
        outpatient_cost_sum = outpatient_costs.groupby(self.patient_id)[outpatient_columns].sum().reset_index()
        return pd.merge(outpatient_trends,outpatient_cost_sum, how='outer',on=self.patient_id )


if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    # patient_ids=Dataset().get_patient_ids()

    ids = ['00013D2EFD8E45D1', '00016F745862898F', '00052705243EA128', '0007F12A492FD25D', '000B97BA2314E971',
           '000C7486B11E7030', '00108066CA1FACCE', '0011714C14B52EEB',
           '0011CB1FE23E91AF', '00139C345A104F72', '0013E139F1F37264', '00157F1570C74E09']

    g = CostFeatures().calculate_batch(ids)
    print(g)
