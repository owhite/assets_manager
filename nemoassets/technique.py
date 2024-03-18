import sys
import db_utils
from base import Base

class Technique(Base):   

    TABLE = "technique"

    # Indicates which fields are required for this table
    # Used for select and insert statements and validation
    FIELDS = {"short_name" : True, "name" : True, "cv_list_id" : False, "date_added" : False}
    
    # These are the fields allowed for use in the constructor
    ATTRS = ["id"] + list(FIELDS.keys())

    def __init__(self, log=None, params={}):   
        super().__init__(log, Technique, params)
    
    def get_technique(self, params:dict):
        return self._get_record(params)
    
    def get_techniques(self, params={}):
        return self._get_records(params)
