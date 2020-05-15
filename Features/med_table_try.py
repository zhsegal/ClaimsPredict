from utils.utils import get_rxcui_from_ndc
import pandas as pd
import multiprocessing as mp
import time
from tqdm import tqdm


def chunkizer(ndc, size):
    for i in range(0, len(ndc), size):
        yield ndc[i:i + size]


class MedTableCreation():
    def __init__(self):
        pass

    def calculate_batch(self, ndc):
        ndc=[self.zero_pad(ndc) for ndc in ndc]

        while True:
            try:
                rxcui_tuples=[get_rxcui_from_ndc(ndc) for ndc in ndc]
                break
            except:
                continue


        results=pd.DataFrame({'ndc':ndc,'rxcui_num':[tuple[0] for tuple in rxcui_tuples],'rxcui_description':[tuple[1] for tuple in rxcui_tuples]})
        return results

    def zero_pad(self,ndc):
        ndc=str(ndc)
        if len(ndc)==7:
            return ('0000' + ndc)
        elif len(ndc)==8:
            return ('000' + ndc)
        elif len(ndc)==9:
            return ('00' + ndc)
        elif len(ndc)==10:
            return ('0' + ndc)
        else:
            return ndc



if __name__ == '__main__':
    pd.set_option('display.max_columns', 81)
    pd.set_option('display.width', 320)

    data= pd.read_csv('Data/Raw_Data/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_1.csv')
    print ('csv_read')
    last=385000
    ndcs=data.PROD_SRVC_ID.unique()[(last+1):]

    for i,ndc in enumerate(chunkizer(ndcs, 100)):
        results=MedTableCreation().calculate_batch(ndc)
        results.to_csv(f'cache/ndc_map_{(i+(last/100+1))*100}.csv')
        print (f'finished_{(i+(last/100+1) )*100}_chunk')
