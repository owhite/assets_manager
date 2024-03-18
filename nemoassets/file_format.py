import sys
import db_utils
from base import Base

class FileFormat(Base):   

    TABLE = "file_format"

    # Indicates which fields are required for this table
    # Used for select and insert statements and validation
    FIELDS = {"format" : True, "cv_list_id" : False, "date_added" : False}
    
    # These are the fields allowed for use in the constructor
    ATTRS = ["id"] + list(FIELDS.keys())

    def __init__(self, log=None, params={}):   
        super().__init__(log, FileFormat, params)
    
    def get_format(self, params={}):
        return self._get_record(params)
    
    def get_formats(self, params={}):
        return self._get_records(params)
