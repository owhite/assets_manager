import sys
import db_utils
from base import Base

class DataType(Base):   

    TABLE = "data_type"

    # Indicates which fields are required for this table
    # Used for select and insert statements and validation
    FIELDS = {"data_type" : True, "cv_list_id" : False, "summary_file" : False, "date_added" : False}
    
    # These are the fields allowed for use in the constructor
    ATTRS = ["id"] + list(FIELDS.keys())

    def __init__(self, log=None, params={}):   
        super().__init__(log, DataType, params)
    
    def get_data_type(self, params:dict):
        return self._get_record(params)
    
    def get_data_types(self, params={}):
        return self._get_records(params)
