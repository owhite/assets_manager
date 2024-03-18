import sys
from base import Base


class Event(Base):

    TABLE = "event"

    # Indicates which fields are required for this table
    # Used for select and insert statements and validation
    FIELDS = {
        "subject_id" : True, "event_name" : True, "event_type" : False, "event_date" : False, "event_info" : False,
        "date_added" : False
    }

    ASSOCIATIONS = {
        "subject_attributes" : {
            "table" : "event_subject_attributes",
            "cols" : ["value", "unit", "attributes_id", "event_id", "source_value"],
            "id_col" : "event_id"
            # "ref_join" : { 
            #     "ref_table" : "attributes",
            #     "ref_field" : "attributes_id",
            #     "readable_field" : "attr_name"
            # }
        }
    }

    # These are the fields allowed for use in the constructor; it includes associations
    ATTRS = ["id"] + list(FIELDS.keys()) + list(ASSOCIATIONS.keys())
             
    def __init__(self, log=None, params={}):   
        super().__init__(log, Event, params)

    def _load_associations(self, event, event_id, delete_before_insert, cursor):

        REQUIRED_ATTRIBUTE_KEYS = ['attr_name', 'value', 'unit']

        # Iterate over the allowed associations
        # if the association is in the library dict
        #   Build the specific dict of column names / values needed for this particular association
        #   Call create_assoc with the assocation info and the column / values dict
        for assoc in self.ASSOCIATIONS:
            values_list = []
            if assoc in event and event[assoc]:
                # make the list of dicts representing this association
                if assoc == "subject_attributes":
                    for attr in event[assoc]:
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
                        values["event_id"] = event_id
                        values_list.append(values)
                else:
                    raise NotImplementedError("This association has not yet been implemented")
                
                if assoc in delete_before_insert:
                    self._delete_assoc(self.ASSOCIATIONS[assoc], cursor)

                for values in values_list:
                    self._create_assoc(self.ASSOCIATIONS[assoc], values, cursor)

    
    def get_all_events(self):
        return self._get_records()
    
    def get_event(self, params, assoc=[]):
        return self._get_record(params, assoc)

    def get_event_by_id(self, event_id):
        return self._get_record({"id" : event_id})
    
    def get_events_for_subject(self, subject_id):
        return self._get_record({"subject_id" : subject_id})
    
    # This function is not used and can be deleted
    def get_next_sampling_event_name(self, subject_id, event_name):
        mysql_cnx = None

        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor(dictionary=True)

            stmt = "SELECT count(id) as cnt FROM event WHERE subject_id = %(subject_id)s AND event_name LIKE %(event_name)s"
            data = { "subject_id" : subject_id, "event_name" : event_name + "_sampling_%" }
            self.log.debug(f"Executing stmt {stmt} with values {data}")
            cursor.execute(stmt, data)
            row = cursor.fetchone()
            next_num = row["cnt"] + 1
            return f"{event_name}_sampling_{next_num}"

        except Exception as error:
            mysql_cnx.rollback()
            self.log.error("Failed in get_next_sampling_event_name() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error
        

    def add_event(self, params={}):
        mysql_cnx = None

        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt, data = self._build_insert_stmt(params)
            self.log.debug(f"Executing stmt {stmt} with values {data}")
            cursor.execute(stmt, data)

            # Get the ID of the record just created to add records to related tables
            id = cursor.lastrowid
            self.log.debug(f"Event ID: {id}")
            
            delete_before_insert = []
            self._load_associations(params, id, delete_before_insert, cursor)

            mysql_cnx.commit()
            return self.get_event({"id" : id})

        except Exception as error:
            mysql_cnx.rollback()
            self.log.error("Failed in add_event() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error
    
    def update_event(self, evt={}):
        raise NotImplementedError("The event.update_event() function hasn't yet been implemented")
    
