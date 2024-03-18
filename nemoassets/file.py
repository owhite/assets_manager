import sys
from datetime import date
from base import Base
import library

class File(Base):

    TABLE = "file"

    # Required (Non-nullable) fields 
    FIELDS = {
        "file_id" : True, "data_type_id" : True, "file_format_id" : True, "project_id" : True,
        "submission_id" : False, "file_name" : True, "md5" : False, "sha256" : False, 
        "size" : False, "mtime" : False, "latest_identifier" : False, "version" : False, 
        "analysis_id" : False, "alt_id" : False, "comment" : False
    }

    ASSOCIATIONS = {
        "data_use_limitations" : {
            "table" : "file_has_data_use_limitation",
            "cols" : ["file_id", "data_use_limitation_id"],
            "id_col" : "file_id"
        },
        "file_attributes" : {
            "table" : "file_attributes",
            "cols" : ["key", "value", "file_id", "cv_list_id"],
            "id_col" : "file_id"
        },
        "analysis_attributes" : { 
            "table" : "analysis_attributes",
            "cols" : ["name", "value", "analysis_id", "file_id"],
            "id_col" : "file_id"
        },
        "file_parents" : {
            "table" : "file_assoc_file",
            "cols" : ["child_file_id", "parent_file_id", "relationship"],
            "id_col" : "parent_file_id"
        },
        "libraries" : {
            "table" : "library_assoc_file",
            "cols" : ["file_id", "library_id"],
            "id_col" : "file_id",
            "ref_join" : { 
                "ref_table" : "library",
                "ref_field" : "library_id",
                "readable_field" : "library_name",
                "ref_cols" : list(library.Library.FIELDS.keys())
            }
        },
        "collections" : {
            "table" : "file_in_collection",
            "cols" : ["file_id", "collection_id"],
            "id_col" : "file_id",
            "ref_join" : { 
                "ref_table" : "collection",
                "ref_field" : "collection_id",
                "readable_field" : "short_name"
            }
        }
    }

    SELF_JOIN_TABLE = {
        "table" : "file_assoc_file",
        "parent_field" : "parent_file_id",
        "child_field" : "child_file_id"
    }

    ATTRS = ["id"] + list(FIELDS.keys()) + list(ASSOCIATIONS.keys())

    def __init__(self, log=None, params={}):
        super().__init__(log, File, params)

    def _load_associations(self, file, file_id, delete_before_insert, cursor):
        
        REQUIRED_FILE_ATTRIBUTE_KEYS = ['key', 'value']
        REQUIRED_ANALYSIS_ATTRIBUTE_KEYS = ['name', 'value', 'analysis_id']
        REQUIRED_ASSOC_FILE_KEYS = ['parent_file_id']
        

        # Iterate over the allowed associations
        # if the association is in the file dict
        #   Build the specific dict of column names / values needed for this particular association
        #   Call create_assoc with the assocation info and the column / values dict
        for assoc in self.ASSOCIATIONS:
            values_list = []
            if assoc in file and file[assoc]:
                # make the data dict representing this association
                if assoc == "file_attributes":
                    for attr in file[assoc]:
                        # each attr is a dict like
                        # {'key' : key, 'value' : value, 'cv_list_id' : cv_list_id}
                        # if this attr dict is missing any of the required keys
                        if not all(key in attr for key in REQUIRED_FILE_ATTRIBUTE_KEYS):
                            raise ValueError(f"File attributes require the following keys: {REQUIRED_FILE_ATTRIBUTE_KEYS}")
                        self.log.debug(f"Processing file attribute {attr['key']}")
                        values = attr
                        values["file_id"] = file_id
                        values_list.append(values)
                elif assoc == "analysis_attributes":
                    for attr in file[assoc]:
                        # each attr is a dict like
                        # {'name' : name, 'value' : value, 'analysis_id' : analysis_id, 'cv_list_id' : cv_list_id}
                        # if this attr dict is missing any of the required keys
                        if not all(key in attr for key in REQUIRED_ANALYSIS_ATTRIBUTE_KEYS):
                            raise ValueError(f"Analysis attributes require the following keys: {REQUIRED_ANALYSIS_ATTRIBUTE_KEYS}")
                        self.log.debug(f"Processing analysis attribute {attr['name']}")
                        # attributes_id = self.db.get_attribute_id_by_name(attr['var_name'])
                        # if not attributes_id:
                            # self.log.error(f"Cannot find attribute with name: {attr['var_name']}")
                            # raise Exception(f"Cannot find attribute with name: {attr['var_name']}")
                        values = attr
                        values["file_id"] = file_id
                        values_list.append(values)
                elif assoc == "file_parents":
                    for parent_dict in file[assoc]:
                        # each parent_dict is a dict like
                        # {'parent_file_id' : parent_file_id, 'relationship' : relationship}
                        # 'relationship' is optional
                        # Check if this parent_dict is missing any of the required keys
                        if not all(key in parent_dict for key in REQUIRED_ASSOC_FILE_KEYS):
                            raise ValueError(f"File parent relationships require the following keys: {REQUIRED_ASSOC_FILE_KEYS}")
                        self.log.debug(f"Processing file parent assoc for file {file_id}")
                        values = parent_dict
                        values["child_file_id"] = file_id
                        values_list.append(values)
                elif assoc == "libraries":
                    for lib_id in file[assoc]:
                        values = { "library_id" : lib_id, "file_id" : file_id }
                        values_list.append(values)
                elif assoc == "file_has_data_use_limitation":
                    raise NotImplementedError(f"This association has not yet been implemented: {assoc}")
                elif assoc == "collections":
                    for short_name in file[assoc]:
                        col_id = self.db.get_collection_id_by_short_name(short_name)
                        values = { "collection_id" : col_id, "file_id" : file_id }
                        values_list.append(values)
                elif assoc == "data_use_limitations":
                    for dul_id in file[assoc]:
                        values = { "data_use_limitation_id" : dul_id, "file_id" : file_id }
                        values_list.append(values)
                else:
                    raise NotImplementedError(f"This association has not yet been implemented: {assoc}")
                
                if assoc in delete_before_insert:
                    self._delete_assoc(self.ASSOCIATIONS[assoc], cursor)

                for values in values_list:
                    self._create_assoc(self.ASSOCIATIONS[assoc], values, cursor)


    @property
    def mtime(self):
        return self._mtime

    @mtime.setter
    def mtime(self, value):
        if value:
            if isinstance(value, str):
                self._mtime = date.fromisoformat(value)
            else:
                self._mtime = value
        else:
            self._mtime = value

    def get_unique_file(self, file_name, md5, project_id):
        return self._get_record({"file_name" : file_name, "md5" : md5, "project_id" : project_id})
                                 
    def get_file(self, params:dict, assoc=[]):
        return self._get_record(params, assoc)
    
    def get_files(self, params={}, assoc={}):
        return self._get_records(params, assoc)
    
    def get_file_by_filename(self, file_name):
        """
        Will raise ValueError if there is more than one file record with this file_name
        This is a real possiblity since file_name is not unique
        """
        return self._get_record({"file_name" : file_name})
    
    def get_file_ancestors(self, flattened=True):
        return self._get_ancestors(flattened)
    
    def add_file(self, file):
        mysql_cnx = None

        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt, data = self._build_insert_stmt(file)
            self.log.debug(f"Executing stmt {stmt} with values {data}")
            cursor.execute(stmt, data)

            # Get the ID of the file just created to add records to related tables
            file_id = cursor.lastrowid
            self.log.debug(f"File ID: {file_id}")
            
            delete_before_insert = []
            self._load_associations(file, file_id, delete_before_insert, cursor)

            mysql_cnx.commit()
            return self.get_file({"id" : file_id})
        
        except Exception as error:
            mysql_cnx.rollback()
            self.log.error("Failed in add_file() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error


    def update_file(self, file:dict, delete_before_insert:list=[]):
        """
        Used to update a file record and associated records in related tables.

        Parameters:
            file (dict): A dict that represents the file object
            delete_before_insert (list): Contains strings such as 'libraries' or
                'file_attributes' which indicates that the associated table's records
                should be wiped for this file and then followed up with inserts.
                In other words, indicates if this update is a wholesale replacement
                for those records.

        Returns:
            None
        """
        mysql_cnx = None

        # Verify that all required fields are specified, else raise exception
        if "id" not in file and "file_id" not in file:
            raise Exception("Required parameters are 'id' or 'file_id'")
        try:

            id = None

            # must have one of these (above)
            if "id" in file:
                file_obj = self.get_file({"id" : file['id']})
            else:
                file_obj = self.get_file({"file_id" : file['file_id']})
                                             
            id = file_obj.id

            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()

            fields_to_update = {}

            # Some fields, like varchars, enums, will be one-to-one updates; whereas things like project_id
            # will need to be retrieved based on the project name
            # We won't include the id field (file_id) because that should remain constant and it is used as a
            # param to this function to uniquely find the sample in the first place
            one_to_one_file_fields = ["data_type_id", "file_format_id", "project_id", "submission_id", "file_name",
                                      "md5", "sha256", "size", "mtime", "latest_identifier", "version", "analysis_id",
                                      "alt_id", "comment"]
        
            for field in one_to_one_file_fields:
                if field in file:
                    fields_to_update[field] = file[field]

            if fields_to_update:
                # Build the query with the fields that we'll be updating
                stmt = "UPDATE " + self.TABLE + " SET "
                stmt +=", ".join([f"{field} = %({field})s" for field in fields_to_update])
                stmt +=" WHERE id = %(id)s"
                
                params = fields_to_update
                params["id"] = id

                self.log.debug(f"Executing stmt: {stmt} with params: {params}" )
                cursor.execute(stmt, params)
            else:
                self.log.debug(f"No columns directly on this object's table need updating.")

            # Optionally delete and then insert the tables associated with this sample
            self._load_associations(file, id, delete_before_insert, cursor)

            mysql_cnx.commit()

        except Exception as error:
            mysql_cnx.rollback()
            self.log.error("Failed in update_file() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error
            



    # Convenience function. Possibly not needed / maybe doesn't make sense to have here
    def get_file_taxonomies(self, file:dict):
        
        mysql_cnx = None
        file_obj = self.get_file(file)

        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor(dictionary=True)

            stmt = """
                SELECT cv_list.id, cv_list.short_name, cv_list.ontology, cv_list.term_id, cv_list.term_name, cv_list.term_definition
                FROM file
                LEFT JOIN library_assoc_file on library_assoc_file.file_id = file.id
                LEFT JOIN library on library.id = library_assoc_file.library_id
                LEFT JOIN sample on library.sample_id = sample.id 
                LEFT JOIN sample_assoc_subject on sample_assoc_subject.sample_id = sample.id
                LEFT JOIN subject on sample_assoc_subject.subject_id = subject.id 
                LEFT JOIN subject_taxonomy on subject_taxonomy.subject_id = subject.id
                LEFT JOIN taxonomy on taxonomy.id = subject_taxonomy.taxonomy_id 
                LEFT JOIN cv_list on cv_list.id = taxonomy.cv_list_id
                WHERE file.id = %(id)s
            """

            data = {"id" : file_obj.id}
            self.log.debug(f"Executing stmt {stmt} with values {data}")
            cursor.execute(stmt, data)
            results = []
            for row in cursor.fetchall():
                results.append(row)

        except Exception as error:
            self.log.error("Failed in get_file_taxonomies() {}".format(error), exc_info=sys.exc_info())
            raise error

