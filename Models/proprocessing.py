import pandas as pd
from sklearn.model_selection import train_test_split
from Features.base_feature import BaseFeature

class PrePrccess(BaseFeature):
    def __init__(self):
        super().__init__()
        self.test_size=0.2


    def preprocess(self, X,y):
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=self.test_size,random_state=self.random_state)
        pass


