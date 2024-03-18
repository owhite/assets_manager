from base import Base

class Analysis(Base):   

    TABLE = "analysis"

    # Indicates which fields are required for this table
    # Used for select and insert statements and validation
    FIELDS = {"name" : True, "cv_list_id" : False, "sop_url" : False, "description" : False, 
              "pipeline_version" : False, "pipeline_container_url" : False, "data_type_specific_tool" : False,
              "date_added" : False}
    
    # These are the fields allowed for use in the constructor
    ATTRS = ["id"] + list(FIELDS.keys())

    def __init__(self, log=None, params={}):
        super().__init__(log, Analysis, params)

    def get_analysis(self, params={}):
        return self._get_record(params)
        
    def get_analyses(self, params={}):
        return self._get_records(params)
