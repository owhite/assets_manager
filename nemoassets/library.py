import sys
import itertools
from base import Base

class Library(Base):

    TABLE = "library"

    # Indicates which fields are required for this table
    # Used for select and insert statements and validation
    FIELDS = {"lib_id" : True, "sample_id" : False, "project_id" : True, "modality_id" : False, "assay_id" : False, 
              "specimen_type_id" : False, "technique_id" : False, "library_name" : False, "batch_name" : False, 
              "alt_id": False, "comment" : False, "date_added" : False, "library_type" : False}
    # "batch_name" : False,
    
    ASSOCIATIONS = {
        # This assoc is used to add associations as well pull the library where child_library.id = library.id
        "lib_assoc_lib_child" : {
            "table" : "library_assoc_library",
            "cols" : ["child_library_id", "parent_library_id"],
            "id_col" : "parent_library_id" # id_col only required for queries so that the generic base.py can figure out the join stmt
        },
        "lib_assoc_lib_parent" : {
            "table" : "library_assoc_library",
            "cols" : ["child_library_id", "parent_library_id"],
            "id_col" : "child_library_id" # id_col only required for queries so that the generic base.py can figure out the join stmt
        },
        "lib_assoc_lib_pool" : {
            "table" : "library_assoc_library_pool",
            "cols" : ["library_id", "library_pool_id"],
            "id_col" : "id" # id_col only required for queries so that the generic base.py can figure out the join stmt
        },
        "attributes" : { 
            "table" : "library_attributes",
            "cols" : ["value", "unit", "attributes_id", "library_id", "source_value"],
            "id_col" : "library_id",
            # "assoc_id_col" : "attributes_id"
        },
        "technique" : {                      # For retrieving associations as part of a library query, add 'technique' into the
            "table" : "technique",           # optional assoc list passed into get_library() or get_libraries(). Joins to table 'technique'
            "cols" : ["id", "short_name"],   # returns the column 'short_name' from technique
            "id_col" : "id",                 # by joining on 'id' from 'technique'
            "assoc_id_col" : "technique_id", # to this table's 'assoc_id_col' (or just 'id' if not specified)
            "one_to_one" : True              # one_to_one set to True causes the result to be returned as a dict instead of list of dicts
        },                                   # where any params are used in to construct the where clause
        "modality" : {                     
            "table" : "modality",          
            "cols" : ["id", "name"],        
            "id_col" : "id",                
            "assoc_id_col" : "modality_id",
            "one_to_one" : True
        },
        "assay" : {                     
            "table" : "assay",          
            "cols" : ["id", "name"],        
            "id_col" : "id",                
            "assoc_id_col" : "assay_id",
            "one_to_one" : True 
        },
        "specimen_type" : {
            "table" : "specimen_type",
            "cols" : ["id", "short_name", "name"],
            "id_col" : "id",
            "assoc_id_col" : "specimen_type_id",
            "one_to_one" : True
        }
    }

    SELF_JOIN_TABLE = {
        "table" : "library_assoc_library",
        "parent_field" : "parent_library_id",
        "child_field" : "child_library_id"
    }

    # These are the fields allowed for use in the constructor; it includes associations
    ATTRS = ["id"] + list(FIELDS.keys()) + list(ASSOCIATIONS.keys())

    def __init__(self, log=None, params={}):   
        super().__init__(log, Library, params)

    def _load_associations(self, library, lib_id, delete_before_insert, cursor):
        
        REQUIRED_LIB_ASSOC_LIB_KEYS = ['child_library_id', 'parent_library_id']
        REQUIRED_ATTRIBUTE_KEYS = ['attr_name', 'value', 'unit']

        # Iterate over the allowed associations
        # if the association is in the library dict
        #   Build the specific dict of column names / values needed for this particular association
        #   Call create_assoc with the assocation info and the column / values dict
        for assoc in self.ASSOCIATIONS:
            values_list = []
            if assoc in library and library[assoc]:
                # make the data dict representing this association
                if assoc == "lib_assoc_lib_child":
                    if isinstance(library[assoc], list):
                        for child_id in library[assoc]:
                            values_list.append({"child_library_id" : child_id, "parent_library_id" : lib_id})
                    else:
                        values_list.append({"child_library_id" : library[assoc], "parent_library_id" : lib_id})
                elif assoc == "lib_assoc_lib_parent":
                    if isinstance(library[assoc], list):
                        for parent_id in library[assoc]:
                            values_list.append({"parent_library_id" : parent_id, "child_library_id" : lib_id})
                    else:
                        values_list.append({"parent_library_id" : library[assoc], "child_library_id" : lib_id})
                elif assoc == "lib_assoc_lib_pool":
                    values_list.append({"library_pool_id" : library[assoc], "library_id" : lib_id})
                elif assoc == "attributes":
                    for attr in library[assoc]:
                        # each attr is a dict like
                        # {'attr_name' : field, 'value' : value, 'unit' : unit, 'source_value' : source_value}
                        # if this attr dict is missing any of the required keys
                        if not all(key in attr for key in REQUIRED_ATTRIBUTE_KEYS):
                            raise ValueError(f"Attributes require the following keys: {REQUIRED_ATTRIBUTE_KEYS}")
                        self.log.debug(f"Processing attribute {attr['attr_name']}")
                        attributes_id = self.db.get_attribute_id_by_name(attr['attr_name'])
                        if not attributes_id:
                            self.log.error(f"Cannot find attribute with name: {attr['attr_name']}")
                            raise Exception(f"Cannot find attribute with name: {attr['attr_name']}")
                        values = attr
                        values["attributes_id"] = attributes_id
                        values["library_id"] = lib_id
                        values_list.append(values)                
                else:
                    raise NotImplementedError(f"This {assoc} association has not yet been implemented")
                
                if assoc in delete_before_insert:
                    self._delete_assoc(self.ASSOCIATIONS[assoc], cursor)

                for values in values_list:
                    self._create_assoc(self.ASSOCIATIONS[assoc], values, cursor)

    def get_all_libraries(self):
        return self._get_records()

    def get_libraries(self, params={}, assoc=[]):
        return self._get_records(params, assoc)
    
    def get_library(self, params:dict, assoc=[]):
        return self._get_record(params, assoc)

    def get_library_by_source_name(self, source_library_name, project_id):
        return self._get_record({"library_name" : source_library_name, "project_id" : project_id})

    def get_library_ancestors(self, flattened=True):
        return self._get_ancestors(flattened)

    def add_library(self, library = {}):
        mysql_cnx = None

        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt, data = self._build_insert_stmt(library)
            self.log.debug(f"Executing stmt {stmt} with values {data}")
            cursor.execute(stmt, data)

            # Get the ID of the library just created to add records to related tables
            lib_id = cursor.lastrowid
            self.log.debug(f"Library ID: {lib_id}")
            
            delete_before_insert = []
            self._load_associations(library, lib_id, delete_before_insert, cursor)

            mysql_cnx.commit()
            return self.get_library({"id" : lib_id})
        
        except Exception as error:
            mysql_cnx.rollback()
            self.log.error("Failed in add_library() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def update_library(self, lib={}):
        raise NotImplementedError("The library.update_library() function hasn't yet been implemented")

    # Convenience function. Possibly not needed / maybe doesn't make sense to have here
    def get_library_taxonomies(self):
        
        mysql_cnx = None
        lib_obj = self

        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor(dictionary=True)

            stmt = """
                SELECT cv_list.id, cv_list.short_name, cv_list.ontology, cv_list.term_id, cv_list.term_name, cv_list.term_definition
                FROM library
                LEFT JOIN sample on library.sample_id = sample.id 
                LEFT JOIN sample_assoc_subject on sample_assoc_subject.sample_id = sample.id
                LEFT JOIN subject on sample_assoc_subject.subject_id = subject.id 
                LEFT JOIN subject_taxonomy on subject_taxonomy.subject_id = subject.id
                LEFT JOIN taxonomy on taxonomy.id = subject_taxonomy.taxonomy_id 
                LEFT JOIN cv_list on cv_list.id = taxonomy.cv_list_id
                WHERE library.id = %(id)s
            """

            data = {"id" : lib_obj.id}
            self.log.debug(f"Executing stmt {stmt} with values {data}")
            cursor.execute(stmt, data)
            results = []
            for row in cursor.fetchall():
                results.append(row)

        except Exception as error:
            self.log.error("Failed in get_library_taxonomies() {}".format(error), exc_info=sys.exc_info())
            raise error

