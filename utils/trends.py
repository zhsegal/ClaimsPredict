import pandas as pd
import numpy as np
from scipy import stats

def get_trends(df, variable_names, variable_column_name, patient_column,date_columns):

    results=df.groupby([patient_column, variable_column_name]).agg({date_columns:[get_last_std,get_trend]})
    g=df[(df.DESYNPUF_ID=='0007F12A492FD25D')&(df.item=='hypertension')]
    trend=get_trend(g)
    std=get_last_std(g)
    pass

def get_trend(dates):
    dates=dates.reset_index().groupby(pd.Grouper(key='CLM_FROM_DT', freq='3M')).count()
    if len(dates)<2:
        return 0
    else:
        x_axis = np.arange(0, len(dates))
        trend = np.polyfit(x_axis, dates, 1)[0]

        return trend

def get_last_std(dates):
    dates = dates.reset_index().groupby(pd.Grouper(key='CLM_FROM_DT', freq='3M')).count()
    if len(dates)<2:
        return 0
    else:
        zscores=stats.zscore(dates)
        return zscores[-1:]