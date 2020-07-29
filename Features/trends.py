import pandas as pd
import numpy as np
from scipy import stats
from Features.base_feature import BaseFeature
import datetime


class Trends(BaseFeature):
    def __init__(self):
        super().__init__()

    def get_sum_trends(self, df, cost_columns, date_column):

        results = df.groupby(self.patient_id)[[date_column, cost_columns[0]]].apply(lambda x: self.sum_trend(x,date_column, cost_columns[0],))
        return results

    def sum_trend(self, df,date_column, cost_column):
        summed_dates=self.get_sums_by_dates(df,date_column,cost_column)
        if (len(summed_dates) < 2) or sum(summed_dates==0):
            trend=0
            std=0
        else:
            x_axis = np.arange(0, len(summed_dates))
            trend = np.polyfit(x_axis, summed_dates, 1)[0]
            zscores = stats.zscore(summed_dates)
            std=zscores[-1]

        results={'trend':trend,'std':std}


        return pd.Series(results, index=['trend', 'std'])


    def get_sums_by_dates(self, df,date_columns,cost_column):
        mock_df = pd.DataFrame(
            {date_columns: [datetime.datetime(2007, 12, 31), self.train_end_time + datetime.timedelta(days=1)], cost_column:0},
            index=[0, 1])
        df=df.append(mock_df)
        grouped_dates = df.groupby(pd.Grouper(key=date_columns, freq='3M'))[cost_column].sum()
        return grouped_dates[1:-1]


    def get_trends(self, df, variable_column_name, patient_column, date_columns):


        results = df.groupby([patient_column, variable_column_name]).agg(
            {date_columns: [self.std, self.trend]}).reset_index()
        if results.empty:
            return pd.DataFrame({self.patient_id:df[self.patient_id].unique()})
        else:
            results = results.set_index([self.patient_id, self.method_subgrouping_column_name]).stack().unstack([1, 2]).droplevel(0, axis=1)
            results.columns = [' '.join(col).strip() for col in results.columns.values]
            return results.fillna(0)

    def trend(self, dates):
        grouped_dates = self.get_grouped_dates(dates)

        if len(grouped_dates) < 2:
            return 0
        else:
            x_axis = np.arange(0, len(grouped_dates))
            trend = np.polyfit(x_axis, grouped_dates, 1)[0]
            return trend

    def std(self, dates):
        grouped_dates = self.get_grouped_dates(dates)

        if len(grouped_dates) < 2:
            return 0
        else:
            zscores = stats.zscore(grouped_dates)
            return zscores[-1:]

    def get_grouped_dates(self, dates):
        mock_df = pd.DataFrame(
            {'CLM_FROM_DT': [datetime.datetime(2007, 12, 31), self.train_end_time + datetime.timedelta(days=1)]},
            index=[0, 1])
        dates = dates.reset_index(name='CLM_FROM_DT')
        dates = dates.append(mock_df)
        grouped_dates = dates.groupby(pd.Grouper(key='CLM_FROM_DT', freq='3M')).count()
        return grouped_dates[1:-1]['index']

    def calculate_compliance(self, df, date_column,compliance_meds):
        df=df[df[self.method_subgrouping_column_name].isin(compliance_meds)]
        results = df.groupby([self.patient_id,self.method_subgrouping_column_name])[date_column, 'DAYS_SUPLY_NUM'].apply(lambda x: self.compliance(x,date_column, 'DAYS_SUPLY_NUM')).reset_index()
        results = results.set_index([self.patient_id, 'Group']).stack().unstack(
            [1, 2]).droplevel(1, axis=1)
        results.columns = [col + '_compliance_score' for col in results.columns]
        return results


    def compliance(self, df,date_column, day_count_column):
        summed_dates=self.method_subgrouping_column_name=df.groupby(pd.Grouper(key=date_column, freq='1M'))[day_count_column].sum()
        zero_ratio=summed_dates.isin([0]).sum()/len(summed_dates)
        if ((zero_ratio > 0.9) or (len(summed_dates)<4)):
            return np.nan
        else:
            compliant_months=[1 if self.get_range(summed_dates[i-3:i],2)[0] <= cal <= self.get_range(summed_dates[i-3:i],2)[1] else 0 for i,cal  in enumerate(summed_dates)]
            compliant_proportion=sum(compliant_months[3:])/len(compliant_months[3:])
            return compliant_proportion

    def get_range(self, series, std_num):
        mean=np.mean(series)
        std=np.std(series)
        return mean-std_num*std,mean+std_num*std