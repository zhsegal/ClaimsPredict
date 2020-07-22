from DB_creation.db_creators import MedicationsDB, BaseDB,BeneficiaryDB,OutpatientDB,InpatientDB,CarrierDB
import os

class DBRunner(BaseDB):
    def __init__(self):
        super().__init__()
        

    def run_dbs(self):
        self.create_medication_db()
        self.create_beneficiary_db()
        self.create_outpatient_db()
        self.create_inpatient_db()
        self.create_carrier_db()
        print ('all dbs created')

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
        for method in ['diagnosis','procedure','hcpcs','costs']:
            method_name = self.get_method_name(method)
            table_name=self.outpatient_db_title
            db_table_name=self.get_db_table_name(table_name, method_name)
            if os.path.exists(self.db_name_to_path(db_table_name)):
                print(f'table {db_table_name} exists')
            else:
                OutpatientDB().make_sql(method)


    def create_inpatient_db(self):
        for method in ['diagnosis','procedure','hcpcs','costs','hospitalizations']:
            method_name = self.get_method_name(method)
            table_name=self.inpatient_db_title
            db_table_name=self.get_db_table_name(table_name, method_name)
            if os.path.exists(self.db_name_to_path(db_table_name)):
                print(f'table {db_table_name} exists')
            else:
                InpatientDB().make_sql(method)

    def create_carrier_db(self):
        for method in ['diagnosis','procedure','hcpcs','deductible_costs','NCH_costs','primary_care_costs',
                       'coinsurance_costs','allowd_chargae']:
            method_name = self.get_method_name(method)
            table_name = self.carrier_db_title
            db_table_name = self.get_db_table_name(table_name, method_name)
            if os.path.exists(self.db_name_to_path(db_table_name)):
                print(f'table {db_table_name} exists')
            else:
                CarrierDB().make_sql(method)



if __name__ == '__main__':
    DBRunner().run_dbs()