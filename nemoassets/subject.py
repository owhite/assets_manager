import sys
import project, cohort
from base import Base

class Subject(Base):   

    TABLE = "subject"
    
    # Indicates which fields are required for this table
    # Used for select and insert statements and validation
    FIELDS = {
        "sbj_id" : True, 
        "source_subject_id" : False, 
        "subject_source" : False, 
        "project_id" : True,
        "cohort_id" : True,
        "subject_type" : False, 
        "comment" : False, 
        "subject_name" : True,
        "alt_id" : False,
        "date_added" : False
    }

    ASSOCIATIONS = {
        "taxonomies" : {
            "table" : "subject_taxonomy",
            "cols" : ["subject_id", "taxonomy_id"],
            "id_col" : "subject_id",
            "ref_join" : { 
                "ref_table" : "taxonomy",
                "ref_field" : "taxonomy_id",
                "readable_field" : "name"
            }
        },
        "attributes" : { 
            "table" : "subject_attributes",
            "cols" : ["value", "unit", "attributes_id", "subject_id", "source_value"],
            "id_col" : "subject_id"
        }
    }

    REQUIRED_KEYS_FOR_ADD = [
        "sbj_id",
        ("project", "project_id"),
        ("cohort", "cohort_id"),
        "subject_name"
    ]


    # These are the fields allowed for use in the constructor; it includes associations
    ATTRS = ["id"] + list(FIELDS.keys()) + list(ASSOCIATIONS.keys())

    
    def __init__(self, log=None, params={}):   
        super().__init__(log, Subject, params)

    def _create_attribute_associations(self, subject_id, attribute_list, cursor):
        """
        Create attribute associations for this subject. This method is called in the context of an existing 
        transaction, so it does not commit the changes. The calling method should ensure changes are committed.

        :param subject_id: The internal database ID for the subject
        :param attribute_list: The list of attributes as dictionaries
        :param cursor: An cursor object on the connection.
        :return: None
        """
        for attr in attribute_list:
            required_keys = ['attr_name', 'value', 'unit']
            # if missing any of these required keys...
            if not all(key in attr for key in required_keys):
                raise ValueError(f"attributes require the following keys: {required_keys}. Received {attr}")
            
            self.log.debug(f"Processing attribute {attr['attr_name']}")
            
            attributes_id = self.db.get_attribute_id_by_name(attr['attr_name'])

            if not attributes_id:
                self.log.error(f"Cannot find attribute with name: {attr['attr_name']}")
                raise Exception(f"Cannot find attribute with name: {attr['attr_name']}")

            stmt = """
                INSERT INTO subject_attributes (value, unit, subject_id, attributes_id, source_value)
                VALUES (%s, %s, %s, %s, %s)
            """
            source_value = None if 'source_value' not in attr else attr['source_value']
            data = (attr['value'], attr['unit'], subject_id, attributes_id, source_value)
            self.log.debug(f"Executing stmt: {stmt} with params: {data}")
            cursor.execute(stmt, data)



    def _create_taxonomy_associations(self, subject_id, taxonomy_list, cursor):
        """
        Create taxonomy associations for this subject. This method is called in the context of an existing 
        transaction, so it does not commit the changes. The calling method should ensure changes are committed.

        :param subject_id: The internal database ID for the subject
        :param taxonomy_list: The list of taxonomy as short_names from cv_list
        :param cursor: An cursor object on the connection.
        :return: None
        """
        for taxon in taxonomy_list:
            self.log.debug(f"Processing taxonomy {taxon}")
            taxonomy_id = self.db.get_taxonomy_id(taxon)

            if not taxonomy_id:
                self.log.error(f"Cannot find taxonomy with name: {taxon}")
                raise Exception(f"Cannot find taxonomy with name: {taxon}")

            stmt = """INSERT INTO subject_taxonomy (subject_id, taxonomy_id)
                            VALUES (%s, %s)
            """
            data = (subject_id, taxonomy_id)
            self.log.debug("Executing stmt: " + stmt)
            cursor.execute(stmt, data)

    def get_all_subjects(self):
        return self._get_subjects()

    def get_subject(self, params:dict, assoc=[]):
        return self._get_record(params, assoc)
    
    def get_subjects(self, params:dict, assoc=[]):
        return self._get_records(params, assoc)
    
    def get_subject_by_source_subject_id(self, source_subject_id):
        return self.get_subjects({"source_subject_id" : source_subject_id})
    
    def add_subject(self, sbj:dict):
        mysql_cnx = None

        # Verify that all required fields are specified, else raise exception
        if not self._validate_add_keys(sbj):
            raise Exception(f"Missing one of required params: {self.REQUIRED_KEYS_FOR_ADD}. Received: {sbj}")

        try:

            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()

            # Fetch the project object and get the ID. If project cannot be
            # found then raise error
            if "project_id" not in sbj:
                p = project.Project(self.log)
                proj_obj = p.get_project_by_name(sbj["project"])
                if proj_obj is None:
                    raise Exception(f"Unable to retrieve project {sbj['project']}")
                project_id = proj_obj.id
            else:
                project_id = sbj["project_id"]

            self.log.debug(f"Project ID: {project_id}")

            # Fetch the cohort object and get the ID. If cohort cannot be
            # found then raise error
            if "cohort_id" not in sbj:
                c = cohort.Cohort(self.log)
                coh_obj = c.get_cohort({"cohort_name" : sbj["cohort"]})
                if coh_obj is None:
                    raise Exception(f"Unable to retrieve cohort {sbj['cohort']}")
                cohort_id = coh_obj.id
            else:
                cohort_id = sbj["cohort_id"]

            self.log.debug(f"Cohort ID: {cohort_id}")

            # Build the query with required fields
            ins_stmt = "INSERT INTO subject (sbj_id, subject_name, project_id, cohort_id"
            val_stmt = "VALUES(%s, %s, %s, %s"

            data = (sbj['sbj_id'], sbj['subject_name'], project_id, cohort_id)

            if "subject_source" in sbj:
                ins_stmt += ", subject_source"
                val_stmt += ", %s"
                data = (*data, sbj['subject_source'])
            
            if "source_subject_id" in sbj:
                ins_stmt += ", source_subject_id"
                val_stmt += ", %s"
                data = (*data, sbj['source_subject_id'])

            # add to statement with optional fields
            if 'subject_type' in sbj:
                ins_stmt = ins_stmt + ", subject_type"
                val_stmt = val_stmt + ", %s"
                data = (*data, sbj['subject_type'])

            if 'alt_id' in sbj:
                ins_stmt = ins_stmt + ", alt_id"
                val_stmt = val_stmt + ", %s"
                data = (*data, sbj['alt_id'])

            if 'comment' in sbj:
                ins_stmt = ins_stmt + ", comment"
                val_stmt = val_stmt + ", %s"
                data = (*data, sbj['comment'])

            # Close the SQL statement
            stmt = ins_stmt + ") " + val_stmt + ")"

            self.log.debug("Executing stmt: " + stmt)
            cursor.execute(stmt, data)

            # Get the ID of the subject just created to add records to related tables
            subject_id = cursor.lastrowid
            self.log.debug(f"Subject ID: {subject_id}")

            # Add subject taxonomies if they have been specified
            if 'taxonomies' in sbj and sbj['taxonomies']:
                self.log.debug(f"taxonomies: {sbj['taxonomies']}")
                self._create_taxonomy_associations(subject_id, sbj['taxonomies'], cursor)

            # Add subject attributes if they have been specified
            if 'attributes' in sbj and sbj['attributes']:
                self.log.debug(f"attributes: {sbj['attributes']}")
                self._create_attribute_associations(subject_id, sbj['attributes'], cursor)

            mysql_cnx.commit()
            return self.get_subject({"sbj_id" : sbj["sbj_id"]})
        except Exception as error:
            mysql_cnx.rollback()
            self.log.error("Failed in add_subject() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error



    def update_subject(self, sbj = {}, delete_then_insert=[]):
        """
        Used to update a subject record and associated records in related tables.

        Parameters:
            sbj (dict): A dict that represents the subject object
            delete_then_insert (list): Contains strings such as 'taxonomies' or
                'attributes' which indicates that the associated table's records
                should be wiped for this subject and then followed up with inserts.
                In other words, indicates if this update is a wholesale replacement
                for those records. Otherwise, the associated tables will be updated
                where they can be. Some tables such as 'subject_taxonomy' don't 
                contain a value that can be updated; it's simply the relation of a 
                subject_id and taxonomy_id. A subject can have multiple taxonomies 
                so knowing which one to update would required additional parameters
                such as the old taxon name; and that's currently not supported.

        Returns:
            None
            
        """
        mysql_cnx = None

        # Verify that all required fields are specified, else raise exception
        if "id" not in sbj or "sbj_id" not in sbj:
            raise Exception("Required parameters are 'id' or 'sbj_id'")
        try:

            id = None

            # must have one of these (above); if it's sbj_id, get the id to keep the update queries simple
            if "id" in sbj:
                id = sbj["id"]
            else:
                subject_obj = self.get_subject({"sbj_id" : sbj["sbj_id"]})
                id = subject_obj.id

            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()

            fields_to_update = {}

            # Some fields, like varchars, enums, will be one-to-one updates; whereas things like project_id
            # will need to be retrieved based on the project name
            # We won't include the id field (sbj_id) because that should remain constant and it is used as a
            # param to this function to uniquely find the subject in the first place
            one_to_one_subject_fields = ["source_subject_id", "subject_source", "subject_type", "comment"]
        
            for field in one_to_one_subject_fields:
                if field in sbj:
                    fields_to_update[field] = sbj[field]

            # Check if project ID was supplied and if it exists in the project table
            # TODO: Allow for either 'project' (name) OR 'prj_id' in sbj
            if "prj_id" in sbj:
                p = project.Project(self.log)
                proj = p.get_project({"prj_id" : sbj["prj_id"]})
                if not proj:
                    self.log.error(f"Cannot find project with id: {sbj['prj_id']}")
                    raise Exception(f"Cannot find project with id: {sbj['prj_id']}")
                fields_to_update["project_id"] = proj.id
            
            # TODO: Allow for either 'cohort' (name) or 'coh_id' in sbj
            if "cohort" in sbj:
                # Fetch the cohort object and make sure it exists.
                # Otherwise, raise error
                c = cohort.Cohort(self.log)
                coh = c.get_cohort({"cohort_name" : sbj["cohort"]})
                if not coh:
                    self.log.error(f"Cannot find cohort with name: {sbj['cohort']}")
                    raise Exception(f"Cannot find cohort with name: {sbj['cohort']}")
                fields_to_update["cohort_id"] = coh.id
            
            # Build the query with the fields that we'll be updating
            stmt = "UPDATE subject SET "
            stmt +=", ".join([f"{field} = %({field})s" for field in fields_to_update])
            stmt +=" WHERE id = %(id)s"
            
            params = fields_to_update
            params["id"] = id

            self.log.debug(f"Executing stmt: {stmt} with params: {params}" )
            cursor.execute(stmt, params)

            # Optionally delete and then insert the taxonomies associated with this subject
            if 'taxonomies' in sbj and sbj['taxonomies']:
                if 'taxonomies' in delete_then_insert:
                    stmt = "DELETE FROM subject_taxonomy WHERE subject_id = %(subject_id)s"
                    self.log.debug(f"Deleting subject_taxonomy records for subject {id}")
                    self.log.debug(f"Executing stmt: {stmt} with id = {id}")
                    cursor.execute(stmt, {"subject_id" : id})

                for taxon in sbj['taxonomies']:
                    self.log.debug(f"Processing taxonomy {taxon}")
                    taxonomy_id = self.db.get_taxonomy_id(taxon)

                    if not taxonomy_id:
                        self.log.error(f"Cannot find taxonomy with name: {taxon}")
                        raise Exception(f"Cannot find taxonomy with name: {taxon}")

                    stmt = """INSERT INTO subject_taxonomy (subject_id, taxonomy_id)
                                    VALUES (%s, %s)
                    """
                    data = (id, taxonomy_id)
                    self.log.debug("Executing stmt: " + stmt)
                    cursor.execute(stmt, data)

            # Optionally delete and then insert the attributes associated with this subject
            if 'attributes' in sbj and sbj['attributes']:
                if 'attributes' in delete_then_insert:
                    stmt = "DELETE FROM subject_attributes WHERE subject_id = %(subject_id)s"
                    self.log.debug(f"Deleting subject_attributes records for subject {id}")
                    self.log.debug(f"Executing stmt: {stmt} with id = {id}")
                    cursor.execute(stmt, {"subject_id" : id})

                # insert the attributes associated with this subject
                self.log.debug(f"attributes: {sbj['attributes']}")
                for attr in sbj['attributes']:
                    required_keys = ['attr_name', 'value', 'unit']
                    # if missing any of these required keys...
                    if not all(key in attr for key in required_keys):
                        raise ValueError(f"attributes require the following keys: {required_keys}")
                    
                    self.log.debug(f"Processing attribute {attr['attr_name']}")
                    
                    attributes_id = self.db.get_attribute_id_by_name(attr['attr_name'])

                    if not attributes_id:
                        self.log.error(f"Cannot find attribute with name: {attr['attr_name']}")
                        raise Exception(f"Cannot find attribute with name: {attr['attr_name']}")

                    # Check if this is an update or an insuert
                    stmt = """
                        SELECT id FROM subject_attributes WHERE attributes_id = %(attributes_id)s and subject_id = %(subject_id)s
                    """

                    source_value = None if 'source_value' not in attr else attr['source_value']

                    self.log.debug("Executing stmt: " + stmt)
                    cursor.execute(stmt, {'attributes_id' : attributes_id, 'subject_id': id})

                    row = cursor.fetchone()
                    if row:
                        stmt = """
                            UPDATE subject_attributes
                            SET value = %(value)s, unit = %(unit)s, source_value = %(source_value)s
                            WHERE attributes_id = %(attributes_id)s and subject_id = %(subject_id)s
                        """
                        params = { "value" : attr['value'],
                                   "unit" : attr['unit'],
                                   "source_value" : source_value,
                                   "attributes_id" : attributes_id,
                                   "subject_id" : id }
                        self.log.debug(f"Executing stmt: {stmt} with params: {params}")
                        cursor.execute(stmt, params)
                    else:
                        stmt = """
                            INSERT INTO subject_attributes (value, unit, subject_id, attributes_id, source_value)
                            VALUES (%s, %s, %s, %s, %s)
                        """
                        params = (attr['value'], attr['unit'], id, attributes_id, source_value)
                        self.log.debug(f"Executing stmt: {stmt} with params: {params}")
                        cursor.execute(stmt, params)

            mysql_cnx.commit()

        except Exception as error:
            mysql_cnx.rollback()
            self.log.error("Failed in update_subject() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error



    

