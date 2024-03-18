
from base import Base

class Assay(Base):   

    TABLE = "assay"

    # Indicates which fields are required for this table
    # Used for select and insert statements and validation
    FIELDS = {"name" : True, "cv_list_id" : False, "date_added" : False}
    
    # These are the fields allowed for use in the constructor
    ATTRS = ["id"] + list(FIELDS.keys())

    def __init__(self, log=None, params={}):   
        super().__init__(log, Assay, params)

    def get_assay(self, params:dict):
        return self._get_record(params)
        
    def get_assays(self, params={}):
        return self._get_records(params)
