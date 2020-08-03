import pandas as pd
from sklearn.model_selection import train_test_split
from Features.base_feature import BaseFeature
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler,OneHotEncoder


class PrePrccess(BaseFeature):
    def __init__(self):
        super().__init__()
        self.test_size=0.2


    def preprocess(self, X,y):
        X=X.set_index(self.patient_id, drop=True)
        y=y.set_index(self.patient_id, drop=True)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

        numeric_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer()),
            ('scaler', StandardScaler())])

        categorical_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
            ('onehot', OneHotEncoder(handle_unknown='ignore'))])

        numeric_features = X.select_dtypes(include=['int64', 'float64']).columns
        categorical_features = X.select_dtypes(include=['object','category']).columns

        pass

    def impute_missing(self, X):
        pass
    
    

