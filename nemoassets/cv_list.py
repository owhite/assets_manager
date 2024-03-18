from base import Base

class CVList(Base):   

    TABLE = "cv_list"

    # Indicates which fields are required for this table
    # Used for select and insert statements and validation
    FIELDS = {"short_name" : True, "ontology" : False, "term_id" : False, "term_name" : False, "term_definition" : False}

    ASSOCIATIONS = {
        "anatomies" : {             # For retrieving associations as part of a cv_list query, add 'anatomies' into the
            "table" : "anatomy",    # optional assoc list passed into get_cv() or get_cvs(). Joins to table 'anatomy'
            "cols" : ["id", "name"],# returns the columns 'id' and 'name from anatomy
            "id_col" : "cv_list_id" # by joining on cv_list_id from anatomy against this (cv_term's) id
                                    # where any params are used in to construct the where clause
        }
    }

    # These are the fields allowed for use in the constructor; it includes associations
    ATTRS = ["id"] + list(FIELDS.keys()) + list(ASSOCIATIONS.keys())

    def __init__(self, log=None, params={}):   
        super().__init__(log, CVList, params)

    def get_cv(self, params={}, assoc=[]):
        return self._get_record(params, assoc)
        
    def get_cvs(self, params={}, assoc=[]):
        return self._get_records(params, assoc)
