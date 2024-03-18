from base import Base

class Attribute(Base):   

    TABLE = "attributes"

    # Indicates which fields are required for this table
    # Used for select and insert statements and validation
    FIELDS = {"attr_name" : True, "attr_type" : True, "category" : False, "cv_list_id" : False, "date_added" : False}
    
    
    # These are the fields allowed for use in the constructor
    ATTRS = ["id"] + list(FIELDS.keys())

    def __init__(self, log=None, params={}):   
        super().__init__(log, Attribute, params)
    
    def get_attribute(self, params:dict):
        return self._get_record(params)
    
    def get_attributes(self, params={}):
        return self._get_records(params)
