from DB_creation.db_creators import MedicationsDB, BaseDB,BeneficiaryDB,OutpatientDB
import os

class DBRunner(BaseDB):
    def __init__(self):
        super().__init__()
        

    def run_dbs(self):
        self.create_medication_db()
        self.create_beneficiary_db()
        self.create_outpatient_db()
        pass

    def create_medication_db(self):
        if os.path.exists(self.db_name_to_path(self.medicaion_db_name)):
            print ('medication db exists')
        else:
            print('medication db doesnt exists, calculting')
            MedicationsDB().make_sql()
            print('medication db calculted')

    def create_beneficiary_db(self):
        if os.path.exists(self.db_name_to_path(self.beneficiary_db_name_08)):
            print('beneficiary db exists')
        else:
            print('beneficiary db doesnt exists, calculting')
            BeneficiaryDB().make_sql()
            print('beneficiary db calculted')

    def create_outpatient_db(self):
        OutpatientDB().make_sql()


if __name__ == '__main__':
    DBRunner().run_dbs()