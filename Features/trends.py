import pandas as pd
import numpy as np
from scipy import stats
from Features.base_feature import BaseFeature
import datetime


class Trends(BaseFeature):
    def __init__(self):
        super().__init__()

    def get_sum_trends(self, df, cost_columns, date_column):

        results = df.groupby(self.patient_id)[date_column, cost_columns[0]].apply(lambda x: self.sum_trend(x,date_column, cost_columns[0],))
        return results

    def sum_trend(self, df,date_column, cost_column):
        summed_dates=self.get_sums_by_dates(df,date_column,cost_column)
        if len(summed_dates) < 2:
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
        results = results.set_index(['DESYNPUF_ID', 'item']).stack().unstack([1, 2]).droplevel(0, axis=1)
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
