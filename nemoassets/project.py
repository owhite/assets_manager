import sys
import program, db_utils

from base import Base

# TODO: This class is deprecated and only here to support the deprecated functions
class GenericProject:
    def __init__(self, **entries):
        self.__dict__.update(entries)

class Project(Base):
    """
    The project class models a project and has the following properties.

        id: Unique numeric ID assigned by database - Integer
        prj_id: Unique alphanumeric project identifier - String (required)
        project_type: Project type - String (required)
        program: Program name associated with this project - String (required)
        short_name: Project short name - String (required)
        title: Project title - String (required)
        description: Project description - String
        url_knowledgebase: URL for any associated knowledgebase entry - String
        date_added: Date and time record was created. Assigned by the database - Datetime
    """

    TABLE = "project"

    # Indicates which fields are required for this table
    # Used for select and insert statements and validation
    FIELDS = {"prj_id" : True, "project_type" : True, "is_grant" : False, "program_id" : True,
              "short_name" : True, "title" : False, "description" : False, "url_knowledgebase" : False,
              "comment" : False, "date_added" : False}

    ASSOCIATIONS = {
        "program" : {
            "table" : "program",
            "cols" : ["id", "prg_id", "name", "rrid"],
            "id_col" : "id",
            "assoc_id_col" : "program_id",
            "one_to_one" : True
        },
        "grant" : {
            "table" : "grant_info",
            "cols" : ["grant_number", "funding_agency", "description_url", "start_date", "end_date", "lead_pi_contributor_id"],
            "id_col" : "project_id",
        },
        "labs" : {
            "table" : "project_assoc_lab",
            "cols" : ["lab_id", "project_id"],
            "id_col" : "project_id",
            "ref_join" : { 
                "ref_table" : "lab",
                "ref_field" : "lab_id",
                "readable_field" : "lab_name"
            }
        },
        "attributes" : { 
            "table" : "project_attributes",
            "cols" : ["name", "value"],
            "id_col" : "project_id",
        },
        "contributors" : {
            "table" : "project_has_contributor",
            "cols" : ["contrib_id", "project_id"],
            "id_col" : "project_id",
            "ref_join" : { 
                "ref_table" : "contributor",
                "ref_field" : "contrib_id",
                "readable_field" : "name" # needs to change, but this is the only unique field in the contributor table
            }
        },
    }

    SELF_JOIN_TABLE = {
        "table" : "project_assoc_project",
        "parent_field" : "parent_project_id",
        "child_field" : "child_project_id"
    }

    # These are the fields allowed for use in the constructor
    ATTRS = ["id"] + list(FIELDS.keys()) + list(ASSOCIATIONS.keys())

    def __init__(self, log=None, params={}):  
        super().__init__(log, Project, params)
        
        # These attributes held over for now while deprecated functions are in place.
        self.db = db_utils.Db(log)

    def get_project(self, params:dict, assoc=[]):
        params["is_grant"] = 0
        return self._get_record(params, assoc)
    
    def get_projects(self, params={}, assoc=[]):
        params["is_grant"] = 0
        return self._get_records(params, assoc)
    
    def get_project_ancestors(self, flattened=True):
        return self._get_ancestors(flattened)


    #######################################################################################
    # Original functions from prior to refactoring this class to extend the Base class    #
    # These functions should be considered deprecated and therefore not used for new code #
    #######################################################################################
    def get_project_by_id(self, id, cnx = None):
        """
        Retrieve project using the unique project database identifier

        :param id: Unique DB idenitifier assigned to project record
        :return: A project object initialized with the properties.
        """
        mysql_cnx = None
        is_grant = 0 
        try:
            # Use a connection if it is already provided, else create a new connection
            if cnx:
                mysql_cnx = cnx
            else:
                mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """ SELECT p.id, prj_id, project_type, m.name as program, short_name, title, description, url_knowledgebase, p.date_added
                        FROM project p
                            JOIN program m on m.id = p.program_id
                        WHERE p.id = %(id)s 
                    """

            params = {'id': id}
            self.log.debug(f"Executing stmt {stmt} with params {params}")
            cursor.execute(stmt, params)

            row = cursor.fetchone()
            if row is not None:
                proj = {}
                proj["id"] = row[0]
                proj["prj_id"] = row[1]
                proj["project_type"] = row[2]
                proj["program"] = row[3]
                proj["short_name"] = row[4]
                proj["title"] = row[5]
                proj["description"] = row[6]
                proj["url_knowledgebase"] = row[7]
                proj["date_added"] = row[8]

                self.log.debug("Returning project with DB ID: {}".format(id))
                
                proj["log"] = self.log
                return GenericProject(**proj)
            else:
                return None

        except Exception as error:
            self.log.error("Failed in get_project_by_id() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                if not cnx:
                    self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def get_project_by_name(self, short_name):
        """
        Retrieve project using the project short name

        :param short_name: Project short name
        :return: A project object initialized with the properties.
        """
        self.log.debug("Retrieving project with name: {}".format(short_name))
        mysql_cnx = None
        is_grant = 0
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """ SELECT p.id, prj_id, project_type, m.name as program, short_name, title, description, url_knowledgebase, p.date_added
                        FROM project p
                            JOIN program m on m.id = p.program_id
                        WHERE short_name = %(short_name)s AND is_grant = %(is_grant)s
                    """

            params = {'short_name': short_name, 'is_grant': is_grant}
            self.log.debug(f"Executing stmt: {stmt} with params {params}")
            cursor.execute(stmt, params)

            project = {}
            row = cursor.fetchone()
            if row is not None:
                proj = {}
                proj["id"] = row[0]
                proj["prj_id"] = row[1]
                proj["project_type"] = row[2]
                proj["program"] = row[3]
                proj["short_name"] = row[4]
                proj["title"] = row[5]
                proj["description"] = row[6]
                proj["url_knowledgebase"] = row[7]
                proj["date_added"] = row[8]

                self.log.debug("Returning project with name: {}".format(short_name))

                proj["log"] = self.log
                return GenericProject(**proj)
            else:
                return None

        except Exception as error:
            self.log.error("Failed in get_project() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def get_grant_or_project_by_name(self, short_name):
        """
        Retrieve project or grant using the short name

        :param short_name: Project short name
        :return: A project object initialized with the properties.
        """
        self.log.debug("Retrieving project with name: {}".format(short_name))
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """ SELECT p.id, prj_id, project_type, m.name as program, short_name, title, description, url_knowledgebase, p.date_added
                        FROM project p
                            JOIN program m on m.id = p.program_id
                        WHERE short_name = %(short_name)s
                    """

            params = {'short_name': short_name}
            self.log.debug(f"Executing stmt: {stmt} with params {params}")
            cursor.execute(stmt, params)

            project = {}
            row = cursor.fetchone()
            if row is not None:
                proj = {}
                proj["id"] = row[0]
                proj["prj_id"] = row[1]
                proj["project_type"] = row[2]
                proj["program"] = row[3]
                proj["short_name"] = row[4]
                proj["title"] = row[5]
                proj["description"] = row[6]
                proj["url_knowledgebase"] = row[7]
                proj["date_added"] = row[8]

                self.log.debug("Returning project with name: {}".format(short_name))
                
                proj["log"] = self.log
                return GenericProject(**proj)
            else:
                return None

        except Exception as error:
            self.log.error("Failed in get_grant_or_project_by_name() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def get_project_labs(self, prj_id):
        """
        Retrieve project and associated labs using the project identifier

        :param prj_id: Unique alphanumeric project ID assigned to project
        :return: A dict of results including the project fields and lab_name
        """
        mysql_cnx = None
        is_grant = 0 
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor(dictionary=True)

            stmt = """ 
                SELECT p.id, p.prj_id, p.project_type, p.program_id, p.short_name, p.title, p.description, p.url_knowledgebase, p.date_added, l.lab_name
                FROM project p, project_assoc_lab pal, lab l
                WHERE p.prj_id = %(prj_id)s 
                    and p.id = pal.project_id
                    and l.id = pal.lab_id
                """
                    # We are dropping the requirement that labs are only associated with a grant
                    # and p.is_grant = %(is_grant)s

            params = {'prj_id': prj_id, 'is_grant': is_grant}
            self.log.debug(f"Executing stmt: {stmt} with params {params}")
            cursor.execute(stmt, params)

            rows = cursor.fetchall()
            self.log.debug("Returning list of project dicts with ID: {}".format(prj_id))
            return rows

        except Exception as error:
            self.log.error("Failed in get_project_labs() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def get_project_assoc_project(self, prj_id):
        """
        Retrieve project associations with other projects using the project (parent) identifier

        :param prj_id: Unique alphanumeric project ID assigned to project
        :return: A dict of results including the project fields and lab_name
        """
        mysql_cnx = None
        is_grant = 0 
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor(dictionary=True)

            stmt = """ 
                SELECT pap.id, pap.parent_project_id, pap.child_project_id
                FROM project_assoc_project pap, project p
                WHERE p.prj_id = %(prj_id)s and pap.parent_project_id = p.id
                """

            self.log.debug("Executing stmt: " + stmt + " with param: " + str(prj_id))
            cursor.execute(stmt, {'prj_id': prj_id})

            rows = cursor.fetchall()
            self.log.debug("Returning list of project_assoc_project dicts with ID: {}".format(prj_id))
            return rows

        except Exception as error:
            self.log.error("Failed in get_project_assoc_project() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

           
    def add_project(self, proj={}):
        """
        Create a project entry in the database with the specified dictionary with the following 
        entities.

        :param proj: A dictionary of the properties of this project with the following keys

            prj_id: Unique alphanumeric project identifier - String (required)
            project_type: Project type - String (required)
            program: Program name associated with this project - String (required)
            short_name: Project short name - String (required)
            title: Project title - String (required)
            description: Project description - String
            url_knowledgebase: URL for any associated knowledgebase entry - String

        :return: No return value
        """
        mysql_cnx = None

        # Verify that all required fields are specified, else raise exception
        if "prj_id" not in proj or "project_type" not in proj or "program" not in proj or "short_name" not in proj or "title" not in proj:
            raise Exception("Missing parameters, prj_id, project_type, program, short_name, and title are required.")
        try:
            
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()

            # Fetch the program object and get the ID. If program cannot be
            # found then raise error
            p = program.Program(self.log)
            prog = p.get_program({"name" : proj["program"]})
            if not prog:
                self.log.error(f"Cannot find program with name: {proj['program']}")
                raise Exception(f"Cannot find program: {proj['program']}")

            program_id = prog.id

            # Build the query with required fields
            ins_stmt = "INSERT INTO project (prj_id, project_type, is_grant, program_id, short_name, title"
            val_stmt = "VALUES(%s, %s, %s, %s, %s, %s"

            data = (proj['prj_id'], proj['project_type'], '0', program_id, proj['short_name'], proj['title'])

            # add to statement with optional fields
            if 'description' in proj:
                ins_stmt = ins_stmt + ", description"
                val_stmt = val_stmt + ", %s"
                data = (*data, proj['description'])

            if 'url_knowledgebase' in proj:
                ins_stmt = ins_stmt + ", url_knowledgebase"
                val_stmt = val_stmt + ", %s"
                data = (*data, proj['url_knowledgebase'])

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
        