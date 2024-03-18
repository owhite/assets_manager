import sys
import db_utils, program

from project import Project
from base import Base

# TODO: This class is deprecated and only here to support the deprecated functions
class GenericGrant:
    def __init__(self, **entries):
        self.__dict__.update(entries)

# Because grant and project use the same table, this class inherits
# everything from the Project class with the exception of:
# 1) The original functions (so as to not break existing code)
# 2) The get_grant() and get_grants() functions forces is_grant to be 1
class Grant(Base):
    """
    The class class models a grant, which is a special type of project, and has the following properties.

        id: Unique numeric ID assigned by database - Integer
        prj_id: Unique alphanumeric project identifier - String (required)
        project_type: Project type - String (required)
        program: Program name associated with this project - String (required)
        short_name: Project short name - String (required)
        title: Project title - String (required)
        description: Project description - String
        url_knowledgebase: URL for any associated knowledgebase entry - String
        grant_number: A string that models a unique grant number. - String (required)
        funding_agency: The funding agency name. - String
        description_url: A URL that points to the page describing the grant at the funding agency. - String
        start_date: Grant start date. - Date
        end_date: Grant end date. - Date
        contributor: Grant principal investigator. - String
        date_added: Date and time record was created. Assigned by the database - Datetime
    """

    TABLE = Project().TABLE
    FIELDS = Project().FIELDS
    ASSOCIATIONS = Project().ASSOCIATIONS
    ATTRS = Project().ATTRS

    def __init__(self, log=None, params={}):
        super().__init__(log, Grant, params) 
        
        # These attributes held over for now while deprecated functions are in place.
        self.db = db_utils.Db(log)

    def get_grant(self, params:dict, assoc=[]):
        params["is_grant"] = 1
        return self._get_record(params, assoc)
    
    def get_grants(self, params={}, assoc=[]):
        params["is_grant"] = 1
        return self._get_records(params, assoc)
    

    #######################################################################################
    # Original functions from prior to refactoring this class to extend the Base class    #
    # These functions should be considered deprecated and therefore not used for new code #
    #######################################################################################

    def get_grant_by_name(self, short_name):
        """
        Retrieve grant using the grant short name

        :param short_name: Project short name
        :return: A grant object initialized with the properties.
        """
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """ SELECT p.id as id, prj_id, project_type, m.name as program,
                            short_name, title, description, url_knowledgebase, p.date_added, grant_number,
                            funding_agency, description_url, start_date, end_date, lead_pi_contributor_id 
                        FROM project p
                            JOIN grant_info g ON p.id = g.project_id
                            JOIN program m on m.id = p.program_id
                        WHERE short_name = %(short_name)s AND is_grant = 1
                    """

            self.log.debug("Executing stmt: " + stmt + " with short_name = " + str(short_name))
            cursor.execute(stmt, {'short_name': short_name})

            row = cursor.fetchone()
            if row:
                grn = {}
                grn["id"] = row[0]
                grn["prj_id"] = row[1]
                grn["project_type"] = row[2]
                grn["program"] = row[3]
                grn["short_name"] = row[4]
                grn["title"] = row[5]
                grn["description"] = row[6]
                grn["url_knowledgebase"] = row[7]
                grn["date_added"] = row[8]
                grn["grant_number"] = row[9]
                grn["funding_agency"] = row[10]
                grn["description_url"] = row[11]
                grn["start_date"] = row[12]
                grn["end_date"] = row[13]
                lead_pi_contrib_id = row[14]
                if lead_pi_contrib_id:
                    grn["contributor"] = self.db.get_contributor_name_by_id(lead_pi_contrib_id)

                grn["log"] = self.log
                return GenericGrant(**grn)
            else:
                return None

        except Exception as error:
            self.log.error("Failed in get_grant_by_name() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def get_grant_by_number(self, grant_number):
        """
        Retrieve grant using the grant number

        :param grant_number: Grant number
        :return: A grant object initialized with the properties.
        """
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """ SELECT p.id as id, prj_id, project_type, m.name as program,
                            short_name, title, description, url_knowledgebase, p.date_added, grant_number,
                            funding_agency, description_url, start_date, end_date, lead_pi_contributor_id 
                        FROM project p
                            JOIN grant_info g ON p.id = g.project_id
                            JOIN program m on m.id = p.program_id
                        WHERE grant_number = %(grant_number)s AND is_grant = 1
                    """

            self.log.debug("Executing stmt: " + stmt + " with grant_number = " + str(grant_number))
            cursor.execute(stmt, {'grant_number': grant_number})

            row = cursor.fetchone()
            if row:
                grn = {}
                grn["id"] = row[0]
                grn["prj_id"] = row[1]
                grn["project_type"] = row[2]
                grn["program"] = row[3]
                grn["short_name"] = row[4]
                grn["title"] = row[5]
                grn["description"] = row[6]
                grn["url_knowledgebase"] = row[7]
                grn["date_added"] = row[8]
                grn["grant_number"] = row[9]
                grn["funding_agency"] = row[10]
                grn["description_url"] = row[11]
                grn["start_date"] = row[12]
                grn["end_date"] = row[13]
                lead_pi_contrib_id = row[14]
                if lead_pi_contrib_id:
                    grn["contributor"] = self.db.get_contributor_name_by_id(lead_pi_contrib_id)

                grn["log"] = self.log
                return GenericGrant(**grn)         
            else:
                return None

        except Exception as error:
            self.log.error("Failed in get_grant_by_number() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def add_grant(self, grn ={}):
        """
        Create a grant entry in the database with the parameters specified in the dictionary
        with the following entities:
        
        :param grn: A disctionary of the grant properties with the following keys

            prj_id: Unique alphanumeric project identifier - String (required)
            project_type: Project type - String (required)
            program: Program name associated with this project - String (required)
            short_name: Project short name - String (required)
            title: Project title - String (required)
            description: Project description - String
            url_knowledgebase: URL for any associated knowledgebase entry - String
            grant_number: A string that models a unique grant number. - String (required)
            funding_agency: The funding agency name. - String
            description_url: A URL that points to the page describing the grant at the funding agency. - String
            start_date: Grant start date. - Date
            end_date: Grant end date. - Date
            contributor: Grant principal investigator specified by name. - String
        """
        mysql_cnx = None

        # Verify that all required fields are specified, else raise exception
        if ("prj_id" not in grn  or "project_type" not in grn  or "program" not in grn  or "short_name" not in grn  or "title" not in grn 
                or 'grant_number' not in grn):
            raise Exception("Missing parameters, prj_id, project_type, program, short_name, and title are required.")
        try:
            
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()

            # Set the transaction isolation leve so we can read uncommited data
            stmt = "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
            cursor.execute(stmt)

            # Fetch the program object and get the ID. If program cannot be
            # found then raise error
            p = program.Program(self.log)
            prog = p.get_program({"name" : grn["program"]})
            if not prog:
                self.log.error(f"Cannot find program with name: {grn['program']}")
                raise Exception(f"Cannot find program with name: {grn['program']}")

            program_id = prog.id

            # Build the query with required fields
            ins_stmt = "INSERT INTO project (prj_id, project_type, is_grant, program_id, short_name, title"
            val_stmt = "VALUES(%s, %s, %s, %s, %s, %s"

            data = (grn['prj_id'], grn['project_type'], '1', program_id, grn['short_name'], grn['title'])

            # add to statement with optional fields
            if 'description' in grn:
                ins_stmt = ins_stmt + ", description"
                val_stmt = val_stmt + ", %s"
                data = (*data, grn['description'])

            if 'url_knowledgebase' in grn:
                ins_stmt = ins_stmt + ", url_knowledgebase"
                val_stmt = val_stmt + ", %s"
                data = (*data, grn['url_knowledgebase'])

            # Close the SQL statement
            stmt = ins_stmt + ") " + val_stmt + ")"

            self.log.debug("Executing stmt: " + stmt)
            cursor.execute(stmt, data)
            project_id = cursor.lastrowid
            print(f"Project ID: {project_id}")

            # After the project has been added, add the grant_info record
            if 'contributor' in grn:
                contrib_id = self.db.get_contibutor_id_by_name(grn['contributor'])
                if not contrib_id:
                    self.log.error(f"Cannot find contributor with name: {grn['contributor']}")
                    raise Exception(f"Cannot find contributor with name: {grn['contributor']}")

            # Build the query with required fields
            ins_stmt = "INSERT INTO grant_info (project_id, grant_number"
            val_stmt = "VALUES(%s, %s"
            data = (project_id, grn['grant_number'], )

            # add to statement with optional fields
            if 'funding_agency' in grn:
                ins_stmt = ins_stmt + ", funding_agency"
                val_stmt = val_stmt + ", %s"
                data = (*data, grn['funding_agency'])

            if 'description_url' in grn:
                ins_stmt = ins_stmt + ", description_url"
                val_stmt = val_stmt + ", %s"
                data = (*data, grn['description_url'])

            if 'start_date' in grn:
                ins_stmt = ins_stmt + ", start_date"
                val_stmt = val_stmt + ", %s"
                data = (*data, grn['start_date'])

            if 'end_date' in grn:
                ins_stmt = ins_stmt + ", end_date"
                val_stmt = val_stmt + ", %s"
                data = (*data, grn['end_date'])

            if 'contrib_id' in grn:
                ins_stmt = ins_stmt + ", lead_pi_contributor_id"
                val_stmt = val_stmt + ", %s"
                data = (*data, contrib_id)

            # Close the SQL statement
            stmt = ins_stmt + ") " + val_stmt + ")"            
            self.log.debug("Executing stmt: " + stmt)
            cursor.execute(stmt, data)            
            
            mysql_cnx.commit()

        except Exception as error:
            mysql_cnx.rollback()
            self.log.error("Failed in add_grant() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error
   
