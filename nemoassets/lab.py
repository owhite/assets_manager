from base import Base

class Lab(Base):   

    TABLE = "lab"

    # Indicates which fields are required for this table
    # Used for select and insert statements and validation
    FIELDS = {"lab_name" : True, "lab_pi_contrib_id" : False, "date_added" : False}
    
    # These are the fields allowed for use in the constructor
    ATTRS = ["id"] + list(FIELDS.keys())

    def __init__(self, log=None, params={}):   
        super().__init__(log, Lab, params)
    
    def get_lab(self, params:dict):
        return self._get_record(params)
    
    def get_labs(self, params={}):
        return self._get_records(params)
