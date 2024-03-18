
import sys
from datetime import date
import db_utils, project
from base import Base

class Cohort(Base):
    """
    The cohort class models a cohort associated with a specific project. Typically there can be one or more cohorts with different 
    access controls for a project which are modeled by data use limitations.

        id: Unique numeric ID assigned by database - Integer
        cohort_name: Cohort name - String (required)
        coh_id: Unique alphanumeric cohort identifier - String (required)
        project: The short name of the project associated with this cohort - String (required)
        ins_cert: The alphanumeric institutional certification ID for human data - String
        description: Description of the cohort - String
        embargoed: A flag indicating if data in this cohort are embargoed, indicated by a value of 0 or 1. - Integer
        embargoed_until: A date until which data are embargoed. - Date
        embargo_duration: A duration in days for which data under this cohort are embargoed. - Integer
        is_human: A flag indicating if this cohort is a human cohort to easy queries, indicated by a value of 0 or 1. - Integer
        date_added: Date and time record was created. Assigned by the database - Datetime
    """

    TABLE = "cohort"

    FIELDS = {"cohort_name" : True, "coh_id" : True, "project_id" : True, "ins_cert_id" : False,
              "description" : False, "embargoed" : False, "embargoed_until" : False, "embargo_duration" : False,
              "is_human" : False, "date_added" : False}
    
    ASSOCIATIONS = {
        "project" : {
            "table" : "project",
            "cols" : ["id", "short_name"],
            "id_col" : "id",
            "assoc_id_col" : "project_id",
            "one_to_one" : True
        },
        "ins_cert" : {
            "table" : "ins_cert",
            "cols" : ["id", "name"],
            "id_col" : "id",
            "assoc_id_col" : "ins_cert_id",
            "one_to_one" : True
        }
    }

    # These are the fields allowed for use in the constructor
    ATTRS = ["id"] + list(FIELDS.keys()) + list(ASSOCIATIONS.keys())

    def __init__(self, log=None, params={}):   
        super().__init__(log, Cohort, params)

    def get_cohort(self, params:dict, assoc=[]):
        return self._get_record(params, assoc)
        
    def get_cohorts(self, params={}, assoc=[]):
        return self._get_records(params, assoc)
   
    # @deprecated("Function should be refactored")
    def get_cohort_by_id_and_project(self, coh_id, proj_id):
        """
        In cases where a user supplies a Cohort ID, using this method guarantees we are also checking that the
        giving project id goes with this cohort.
        """
        mysql_cnx = None
        self.log.debug("Retrieving cohort with name: " + coh_id)
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """ SELECT id, cohort_name, coh_id, project_id, ins_cert_id, description, 
                            embargoed, embargoed_until, embargo_duration, is_human, date_added
                        FROM cohort
                        WHERE coh_id = %(coh_id)s and project_id = %(project_id)s
                    """

            values = {"coh_id" : coh_id, "project_id" : proj_id}
            self.log.debug(f"Executing stmt: {stmt} with values {values}")
            cursor.execute(stmt, values)

            row = cursor.fetchone()
            self.log.debug("Retrieved row: " + str(row))
            if row:
                coh = {}
                coh["id"] = row[0]
                coh["cohort_name"] = row[1]
                coh["coh_id"] = row[2]
                coh["project"] = row[3]
                if row[4]:
                    ins_cert_id = row[4]
                    coh["ins_cert"] = self.db.get_ins_cert_by_id(ins_cert_id)
                coh["description"] = row[5]
                coh["embargoed"] = row[6]
                coh["embargoed_until"] = row[7]
                coh["embargo_duration"] = row[8]
                coh["is_human"] = row[9]
                coh["date_added"] = row[10]

                return Cohort(self.log, coh)
            else:
                return None

        except Exception as error:
            self.log.error("Failed in get_cohort_by_id_and_project() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    # @deprecated("Function should be refactored")
    def get_all_cohorts_for_project(self, project_name):
        """
        Retrieve all the cohorts for the given project name.
        """
        mysql_cnx = None
        self.log.debug(f"Retrieving cohorts for project with name: {project_name}")
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """ SELECT c.id, cohort_name, coh_id, project_id, ins_cert_id, c.description, 
                            embargoed, embargoed_until, embargo_duration, is_human, c.date_added
                        FROM cohort c
                        JOIN project p ON c.project_id = p.id
                        WHERE p.short_name = %(project_name)s
                    """

            values = {"project_name" : project_name}
            self.log.debug(f"Executing stmt: {stmt} with values {values}")
            cursor.execute(stmt, values)

            cohorts = []
            for row in cursor.fetchall():
                self.log.debug("Retrieved row: " + str(row))
                coh = {}
                coh["id"] = row[0]
                coh["cohort_name"] = row[1]
                coh["coh_id"] = row[2]
                coh["project"] = row[3]
                if row[4]:
                    ins_cert_id = row[4]
                    coh["ins_cert"] = self.db.get_ins_cert_by_id(ins_cert_id)
                coh["description"] = row[5]
                coh["embargoed"] = row[6]
                coh["embargoed_until"] = row[7]
                coh["embargo_duration"] = row[8]
                coh["is_human"] = row[9]
                coh["date_added"] = row[10]
                cohorts.append(Cohort(self.log, coh))

            return cohorts

        except Exception as error:
            self.log.error("Failed in get_cohort_by_id_and_project() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    # @deprecated("Function should be refactored")
    def add_cohort(self, coh ={}):
        """
        This method adds a cohort record in the database using the variables passed as a dictionary with the 
        following keys:

        :param coh: A dictionary of the properties of this cohort with the following keys

            cohort_name: Cohort name - String (required)
            coh_id: Unique alphanumeric cohort identifier - String (required)
            project: The short name of the project associated with this cohort - String (required)
            ins_cert: The alphanumeric institutional certification ID for human data - String
            description: Description of the cohort - String
            embargoed: A flag indicating if data in this cohort are embargoed, indicated by a value of 0 or 1. - Integer
            embargoed_until: A date until which data are embargoed. - Date
            embargo_duration: A duration in days for which data under this cohort are embargoed. - Integer
            date_added: Date and time record was created. Assigned by the database - Datetime
        
        :return: None
        """
        mysql_cnx = None

        # Verify that all required fields are specified, else raise exception
        if "coh_id" not in coh  or "cohort_name" not in coh or "project" not in coh:
            raise Exception("Missing parameters: coh_id, cohort_name, project.")
        try:
            
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()

            # Retrieve the project_id based on the sname specified as a parameter
            project_id = project.Project(self.log).get_project_by_name(coh['project']).id

            # If the IC alphanumeric is specified retrieve the IC id
            if 'ins_cert' in coh: 
                ins_cert_id = self.db.get_ins_cert_id_by_asset_identifier(coh['ins_cert'])
                if not ins_cert_id:
                    raise Exception("Could not find a matching IC record.")

            # Build the query with required fields
            ins_stmt = "INSERT INTO cohort (coh_id, cohort_name, project_id"
            val_stmt = "VALUES(%s, %s, %s"

            data = (coh['coh_id'], coh['cohort_name'], project_id)

            # add to statement with optional fields
            if 'ins_cert' in coh:
                ins_stmt = ins_stmt + ", ins_cert_id"
                val_stmt = val_stmt + ", %s"
                data = (*data, ins_cert_id)

            if 'description' in coh:
                ins_stmt = ins_stmt + ", description"
                val_stmt = val_stmt + ", %s"
                data = (*data, coh['description'])

            if 'embargoed' in coh:
                ins_stmt = ins_stmt + ", embargoed"
                val_stmt = val_stmt + ", %s"
                data = (*data, coh['embargoed'])

            if 'embargoed_until' in coh:
                ins_stmt = ins_stmt + ", embargoed_until"
                val_stmt = val_stmt + ", %s"
                data = (*data, coh['embargoed_until'])

            if 'embargo_duration' in coh:
                ins_stmt = ins_stmt + ", embargo_duration"
                val_stmt = val_stmt + ", %s"
                data = (*data, coh['embargo_duration'])

            if 'is_human' in coh:
                ins_stmt = ins_stmt + ", is_human"
                val_stmt = val_stmt + ", %s"
                data = (*data, coh['is_human'])

            # Close the SQL statement
            stmt = ins_stmt + ") " + val_stmt + ")"

            self.log.debug("Executing stmt: " + stmt)
            cursor.execute(stmt, data)
            
            mysql_cnx.commit()

        except Exception as error:
            mysql_cnx.rollback()
            self.log.error("Failed in add_project() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error
   
