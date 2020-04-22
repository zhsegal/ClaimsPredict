import multiprocessing as mp
from config.configuration import Configuration
import pandas as pd

class BaseFeature():
    def __init__(self):
        self.config=Configuration().get_config()
        self.batch_size=self.config['multiprocessing']['batch_size']
    def calculate_feature(self, ids):

        added_features = self.run_multiprocess(ids)


    def run_multiprocess(self, ids):
        pool=mp.Pool(processes=self.config['multiprocessing']['process_number'])
        results = []

        def collect_result(result):
            results.append(result)

        for i, id in enumerate(ids):
            pool.apply_async(self.calculate_batch, args=(id), callback=collect_result)

        pool.close()
        pool.join()

    def calculate_batch(self, ids):
        print (type(ids))
        return ids

    def chunkizer(self, ids):
        pass


if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    ids = ['00013D2EFD8E45D1', '00016F745862898F']

    BaseFeature().calculate_feature(ids)