from RealEstateData import RealEstateDataClass
import data_source as ds
import data_normalize as dn
import data_engineering as de

class DataManager:
    def __init__(self):
        self.data_class = None

    def init(self):
        self.data_class = RealEstateDataClass(ds, dn, de, year_earliest=2011)

    def get_data_class(self):
        return self.data_class


data_manager = DataManager()