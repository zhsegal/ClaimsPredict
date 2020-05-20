from Features.run_all_features import RunFeatures
import pandas as pd
from Targets.run_targets import RunTargets
from Models.proprocessing import PrePrccess

def run_models():
    all_features=RunFeatures().get_features()
    targets=RunTargets().get_targets()
    preprocessed=PrePrccess().preprocess(all_features,targets)
    return all_features

if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    g = run_models()
    print(g)
