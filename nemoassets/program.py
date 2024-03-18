import sys
from datetime import date
import db_utils
from base import Base

class Program(Base):

    TABLE = "program"

    # Indicates which fields are required for this table
    # Used for select and insert statements and validation
    FIELDS = {"prg_id" : True, "name" : True, "rrid" : False, "date_added" : False}

    ATTRS = ["id"] + list(FIELDS.keys()) # + list(ASSOCIATIONS.keys())

    def __init__(self, log=None, params={}):   
        super().__init__(log, Program, params)
    
    def get_program(self, params:dict, assoc=[]):
        return self._get_record(params, assoc)
    
    def get_programs(self, params={}, assoc=[]):
        return self._get_records(params, assoc)
