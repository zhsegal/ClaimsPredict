import json
import os


class Configuration():
    def __init__(self):
        self.congif_path='config/config.json'

    def get_config(self):
        os.chdir('C:\\Users\\Boston\\PycharmProjects\\ClaimstoModels')
        with open(self.congif_path, "r") as f:
            self.config_json = json.load(f)

            return self.config_json


