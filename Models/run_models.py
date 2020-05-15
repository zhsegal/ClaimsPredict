from Features.run_all_features import RunFeatures
import pandas as pd

def run_models():
    all_features=RunFeatures().get_features()
    return all_features

if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    g = RunFeatures().get_features()
    print(g)
