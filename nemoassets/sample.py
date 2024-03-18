import sys
import project
import event
from base import Base



class Sample(Base):

    TABLE = "sample"

    # Indicates which fields are required for this table
    # Used for select and insert statements and validation
    FIELDS = {"smp_id" : True, "project_id" : True, "source_sample_id" : False, "sample_source" : False, "sample_type" : False, 
              "event_id" : False, "comment" : False, "sample_name" : False, "alt_id" : False, "date_added" : False}
    
    ASSOCIATIONS = {
        "sbj_ids" : {
            "table" : "sample_assoc_subject",
            "cols" : ["sample_id", "subject_id"], # used by load_associations to know what fields to insert
            "id_col" : "sample_id"
        },
        "attributes" : { 
            "table" : "sample_attributes",
            "cols" : ["value", "unit", "attributes_id", "sample_id", "source_value"],
            "id_col" : "sample_id",
        },
        "anatomies" : { 
            "table" : "sample_assoc_anatomy", 
            "cols" : ["sample_id", "anatomy_id"],
            "id_col" : "sample_id",
            "ref_join" : { 
                "ref_table" : "anatomy",
                "ref_field" : "anatomy_id",
                "readable_field" : "name"
            }
        },
        "sample_assoc_sample_parent" : {
            "table" : "sample_assoc_sample",
            "cols" : ["child_sample_id", "parent_sample_id", "relationship", "root_sample_id"],
            "id_col" : "child_sample_id" # id_col only required for queries so that the generic base.py can figure out the join stmt
        },
        "sample_assoc_sample_child" : {
            "table" : "sample_assoc_sample",
            "cols" : ["child_sample_id", "parent_sample_id", "relationship", "root_sample_id"],
            "id_col" : "parent_sample_id" # id_col only required for queries so that the generic base.py can figure out the join stmt
        }
    }

    # These are the fields allowed for use in the constructor; it includes associations
    ATTRS = ["id"] + list(FIELDS.keys()) + list(ASSOCIATIONS.keys())
             
    def __init__(self, log=None, params={}):   
        super().__init__(log, Sample, params)

    def _load_associations(self, sample, sample_id, delete_before_insert, cursor):

        REQUIRED_ATTRIBUTE_KEYS = ['attr_name', 'value', 'unit']

        # Iterate over the allowed associations
        # if the association is in the sample dict
        #   Build the specific dict of column names / values needed for this particular association
        #   Call create_assoc with the assocation info and the column / values dict
        for assoc in self.ASSOCIATIONS:
            if assoc in sample and sample[assoc]:
                self.log.debug(f"Processing association: {assoc}")
                values_list = []
                # make the data dict representing this association
                if assoc == "sbj_ids":
                    for subject_id in sample[assoc]:
                        values_list.append({"sample_id" : sample_id, "subject_id" : subject_id})
                elif assoc == "attributes":
                    for attr in sample[assoc]:
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
                        values["sample_id"] = sample_id
                        values_list.append(values)
                elif assoc == "anatomies":
                    for anatomy in sample[assoc]:
                        # each anatomy is a short_name string from cv_list
                        # anatomy_id = self.db.get_anatomy_id_by_short_name(anatomy)
                        values_list.append({ "anatomy_id" : anatomy, "sample_id" : sample_id})
                elif assoc == "sample_assoc_sample_parent":
                    for parent_dict in sample[assoc]:
                        values_list.append({"child_sample_id" : sample_id, "parent_sample_id" : parent_dict["parent_sample_id"],
                                            "relationship" : parent_dict["relationship"]})
                else:
                    raise NotImplementedError("This association has not yet been implemented")
                
                if assoc in delete_before_insert:
                    deletion_key = self.ASSOCIATIONS[assoc]['id_col']
                    self.log.debug(f"Deletion key: {deletion_key}")
                    self._delete_assoc(self.ASSOCIATIONS[assoc], deletion_key, sample_id, cursor)
                
                for values in values_list:
                    self._create_assoc(self.ASSOCIATIONS[assoc], values, cursor)

                    
    def get_all_samples(self):
        return self._get_records()

    def get_samples(self, params, assoc=[]):
        return self._get_records(params, assoc)
    
    def get_sample(self, params, assoc=[]):
        return self._get_record(params, assoc)
    
    def get_sample_by_sample_name_and_project(self, source_sample_id, project_id, assoc=[]):
        return self._get_record({ "sample_name": source_sample_id, "project_id" : project_id }, assoc)
    
    
    @DeprecationWarning
    # This function should be removed. I'm not sure we should ever be getting a sample by source id without the project id.
    # Otherwise, it's not a unique sample. Not sure if anyone is using this function.
    def get_sample_by_sourceid(self, source_sample_id):
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """ SELECT id, smp_id, source_sample_id, sample_source, sample_type, event_id, comment, date_added
                        FROM sample
                        WHERE source_sample_id = %(source_sample_id)s
                    """

            self.log.debug("Executing stmt: " + stmt)
            cursor.execute(stmt, {'source_sample_id': source_sample_id})

            row = cursor.fetchone()
            if row:
                sample = {}
                sample["id"] = row[0]
                sample["smp_id"] = row[1]
                sample["source_sample_id"] = row[2]
                sample["sample_source"] = row[3]
                sample["sample_type"] = row[4]
                sample["event_id"] = row[5]
                sample["comment"] = row[6]
                sample["date_added"] = row[7]
                 
                return Sample(self.log, sample)
            else:
                return None

        except Exception as error:
            self.log.error("Failed in get_sample_by_sourceid {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error
    
    
    def add_sample(self, smp:dict):
        mysql_cnx = None

        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt, data = self._build_insert_stmt(smp)
            self.log.debug(f"Executing stmt {stmt} with values {data}")
            cursor.execute(stmt, data)

            # Get the ID of the sample just created to add records to related tables
            sample_id = cursor.lastrowid
            self.log.debug(f"Sample ID: {sample_id}")
            
            delete_before_insert = []
            self._load_associations(smp, sample_id, delete_before_insert, cursor)

            mysql_cnx.commit()
            return self.get_sample({"id" : sample_id})
        except Exception as error:
            mysql_cnx.rollback()
            self.log.error("Failed in add_sample() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error
   
    def update_sample(self, smp:dict, delete_before_insert:list=[]):
        """
        Used to update a sample record and associated records in related tables.

        Parameters:
            smp (dict): A dict that represents the subject object
            delete_before_insert (list): Contains strings such as 'anatomies' or
                'attributes' which indicates that the associated table's records
                should be wiped for this subject and then followed up with inserts.
                In other words, indicates if this update is a wholesale replacement
                for those records.

        Returns:
            None
            
        """
        mysql_cnx = None

        # Verify that all required fields are specified, else raise exception
        if "id" not in smp and "smp_id" not in smp:
            raise Exception("Required parameters are 'id' or 'smp_id'")
        try:

            id = None

            # must have one of these (above); if it's sbj_id, get the id to keep the update queries simple
            if "id" in smp:
                sample_obj = self.get_sample({"id" : smp['id']})
            else:
                sample_obj = self.get_sample({"smp_id" : smp['smp_id']})
                                             
            id = sample_obj.id

            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()

            fields_to_update = {}

            # Some fields, like varchars, enums, will be one-to-one updates; whereas things like project_id
            # will need to be retrieved based on the project name
            # We won't include the id field (smp_id) because that should remain constant and it is used as a
            # param to this function to uniquely find the sample in the first place
            one_to_one_sample_fields = ["sample_source_id", "sample_source", "sample_type", "comment", "alt_id"]
        
            for field in one_to_one_sample_fields:
                if field in smp:
                    fields_to_update[field] = smp[field]

            # If project_id was supplied, validate it exists in the project table
            if "project_id" in smp:
                p = project.Project(self.log)
                proj = p.get_project_by_id(smp["project_id"])
                if not proj:
                    proj = p.get_project_by_id(smp["project_id"], is_grant=1)
                    if not proj:
                        self.log.error(f"Cannot find project with id: {smp['project_id']}")
                        raise Exception(f"Cannot find project with id: {smp['project_id']}")
                fields_to_update["project_id"] = proj.id

            # If event_id was supplied, validate it exists in the project table            
            if "event_id" in smp:
                e = event.Event(self.log)
                e.get_event_by_id(smp["event_id"])
                if e is None:
                    message = f"Cannot update sample to reference event with id: {smp['event_id']} because it does not exist."
                    self.log.error(message)
                    raise Exception(message)

            if fields_to_update:
                # Build the query with the fields that we'll be updating
                stmt = "UPDATE sample SET "
                stmt +=", ".join([f"{field} = %({field})s" for field in fields_to_update])
                stmt +=" WHERE id = %(id)s"
                
                params = fields_to_update
                params["id"] = id

                self.log.debug(f"Executing stmt: {stmt} with params: {params}" )
                cursor.execute(stmt, params)
            else:
                self.log.debug(f"No columns directly on this object's table need updating.")

            # Optionally delete and then insert the tables associated with this sample
            self._load_associations(smp, id, delete_before_insert, cursor)

            mysql_cnx.commit()

        except Exception as error:
            mysql_cnx.rollback()
            self.log.error("Failed in update_sample() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    # Convenience function. Possibly not needed / maybe doesn't make sense to have here
    def get_sample_taxonomies(self, smp:dict):
        
        mysql_cnx = None
        sample_obj = self.get_sample(smp)

        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor(dictionary=True)

            stmt = """
                SELECT cv_list.id, cv_list.short_name, cv_list.ontology, cv_list.term_id, cv_list.term_name, cv_list.term_definition
                FROM sample
                LEFT JOIN sample_assoc_subject on sample_assoc_subject.sample_id = sample.id
                LEFT JOIN subject on sample_assoc_subject.subject_id = subject.id 
                LEFT JOIN subject_taxonomy on subject_taxonomy.subject_id = subject.id
                LEFT JOIN taxonomy on taxonomy.id = subject_taxonomy.taxonomy_id 
                LEFT JOIN cv_list on cv_list.id = taxonomy.cv_list_id
                WHERE sample.id = %(id)s
            """

            data = {"id" : sample_obj.id}
            self.log.debug(f"Executing stmt {stmt} with values {data}")
            cursor.execute(stmt, data)
            results = []
            for row in cursor.fetchall():
                results.append(row)
            return results

        except Exception as error:
            self.log.error("Failed in get_sample_taxonomies() {}".format(error), exc_info=sys.exc_info())
            raise error
        
