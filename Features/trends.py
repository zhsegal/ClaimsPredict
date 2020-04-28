import pandas as pd
import numpy as np
from scipy import stats
from Features.base_feature import BaseFeature
import datetime


class Trends(BaseFeature):
    def __init__(self):
        super().__init__()

    def get_trends(self, df, variable_names, variable_column_name, patient_column, date_columns):

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
