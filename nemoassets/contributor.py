from base import Base

class Contributor(Base):
    """
    The Contributor class models a generic person who is a contributor with the following properties:

        id: Unique numeric ID assigned by database - Integer
        orcid_id: Alphanumeric string specifying the ORCID ID for this individual. - String (required)
        name: Full name of the individual. - String (required)
        email: Email associated with the individual. - String (required)
        organization: Organization with this individual is associated. - String (required)
        lab: A unique name for a laboratory. - String (required)            
        aspera_uname: The username associated with Aspera account. - String 
        lname: The username last name. - String 
        date_added: Date and time record was created. Assigned by the database - Datetime
    """

    TABLE = "contributor"

    # Indicates which fields are required for this table
    # Used for select and insert statements and validation
    FIELDS = {"name" : True, "orcid_id" : False, "email" : False, "organization" : False,
              "aspera_uname" : False, "lab_lab_id" : False, "lname" : False, "date_added" : False}


    ASSOCIATIONS = {
        "lab" : {
            "table" : "lab",
            "cols" : ["id", "lab_name"],
            "id_col" : "id",
            "assoc_id_col" : "lab_lab_id",
            "one_to_one" : True
        }
    }

    # These are the fields allowed for use in the constructor
    ATTRS = ["id"] + list(FIELDS.keys()) + list(ASSOCIATIONS.keys())

    def __init__(self, log=None, params={}):   
        super().__init__(log, Contributor, params)

    def get_contributor(self, params:dict, assoc=[]):
        return self._get_record(params, assoc)
        
    def get_contributors(self, params={}, assoc=[]):
        return self._get_records(params, assoc)

    def add_contributor(self, contrib ={}):
        """
        This method adds a contributor record in the database.

        :param contrib: A dictionary of the properties of this cohort with the following keys
            orcid_id: Alphanumeric string specifying the ORCID ID for this individual. - String (required)
            name: Full name of the individual. - String (required)
            email: Email associated with the individual. - String (required)
            organization: Organization with this individual is associated. - String (required)
            lab: A unique name for a laboratory. - String (required)            
            aspera_uname: The username associated with Aspera account. - String 
            lname: The username last name. - String 

        :return: None
        """ 
        mysql_cnx = None

        # Verify that all required fields are specified, else raise exception
        if "orcid_id" not in contrib  or "name" not in contrib or "email" not in contrib or "organization" not in contrib or "lab" not in contrib:
            raise Exception("Required parameters missing: orcid_id, name, email, organization, lab")
        try:

            # Get the lab_id from the specified lab
            lab_id = self.db.get_lab_id_by_name(contrib['lab'])
            if not lab_id:
                raise Exception("Could not find the ID for the lab with name {}".format(contrib['lab']))

            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()

            # Build the query with required fields
            ins_stmt = "INSERT INTO contributor (orcid_id, name, email, organization, lab_lab_id"
            val_stmt = "VALUES(%s, %s, %s, %s, %s"

            data = (contrib['orcid_id'], contrib['name'], contrib['email'], contrib['organization'], lab_id) 

            # add to statement with optional fields
            if 'aspera_uname' in contrib:
                ins_stmt = ins_stmt + ", aspera_uname"
                val_stmt = val_stmt + ", %s"
                data = (*data, contrib['aspera_uname'])

            if 'lname' in contrib:
                ins_stmt = ins_stmt + ", lname"
                val_stmt = val_stmt + ", %s"
                data = (*data, contrib['lname'])

            # Close the SQL statement
            stmt = ins_stmt + ") " + val_stmt + ")"

            self.log.debug("Executing stmt: " + stmt)
            cursor.execute(stmt, data)

            # Get the ID of the sibject just created to add records to related tables
            subject_id = cursor.lastrowid
            self.log.debug(f"Contributor ID: {subject_id}")
                
            mysql_cnx.commit()

        except Exception as error:
            mysql_cnx.rollback()
            self.log.error("Failed in add_contributor() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error
   
