import sys
from datetime import date
import db_utils
from base import Base

class DataUseLimitation(Base):

    TABLE = "data_use_limitation"

    # Indicates which fields are required for this table
    # Used for select and insert statements and validation
    FIELDS = {"cohort_id" : True, "summary_files" : False, "access" : False, "dul_cv_list_id" : False, 
              "data_use_limit_name" : False, "specific_limit_cv_list_id" : False, "specific_limit" : False,
              "comment" : False, "date_added" : False}

    # THIS IS WRONG, THIS ASSOCATION IS THE OTHER DIRECTION. MAYBE HAVE A DIRECTIONALITY FOR EACH ASSOCIATION SO THE BASE CLASS KNOWS HOW TO JOIN.
    ASSOCIATIONS = {}
    #     # This assoc is used to pull the cohort from the cohort table
    #     "cohort" : {
    #         "table" : "cohort",
    #         "cols" : ["cohort_name"],
    #         "id_col" : "cohort_id" # id_col only required for queries so that the generic base.py can figure out the join stmt
    #     }
    # }

    # These are the fields allowed for use in the constructor; it includes associations
    ATTRS = ["id"] + list(FIELDS.keys()) + list(ASSOCIATIONS.keys())

    def __init__(self, log=None, params={}):   
        super().__init__(log, DataUseLimitation, params)
    
    def get_dul_for_cohort_summary_flag(self, cohort_id:int, summary_flag:int) -> 'DataUseLimitation':
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor(dictionary=True, buffered=True)

            stmt = """
                SELECT d.*
                FROM data_use_limitation d
                LEFT JOIN cv_list c on d.dul_cv_list_id = c.id
                WHERE d.cohort_id = %(cohort_id)s 
                    and d.summary_files = %(summary_flag)s;
            """

            params = {'cohort_id' : cohort_id, "summary_flag" : summary_flag}
            self.log.debug(f"Executing stmt: {stmt} with params {params}")
            cursor.execute(stmt, params)

            row = cursor.fetchone()
            if row:
                return DataUseLimitation(self.log, row)
            return None

        except Exception as error:
            self.log.error("Failed in get_dul_for_cohort_summary_flag() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                cursor.close()
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error        

    def get_all_duls_for_cohort_summary_flag(self, cohort_id:int, summary_flag:int) -> 'DataUseLimitation':
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor(dictionary=True, buffered=True)

            stmt = """
                SELECT d.*
                FROM data_use_limitation d
                LEFT JOIN cv_list c on d.dul_cv_list_id = c.id
                WHERE d.cohort_id = %(cohort_id)s 
                    and d.summary_files = %(summary_flag)s;
            """

            params = {'cohort_id' : cohort_id, "summary_flag" : summary_flag}
            self.log.debug(f"Executing stmt: {stmt} with params {params}")
            cursor.execute(stmt, params)

            duls = []
            for row in cursor.fetchall():
                dul = DataUseLimitation(self.log, row)
                duls.append(dul)
            
            return duls 

        except Exception as error:
            self.log.error("Failed in get_dul_for_cohort_summary_flag() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                cursor.close()
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error    

    def get_dul_for_cohort_summary_flag_and_access(self, cohort_id:int, summary_flag:int, access) -> 'DataUseLimitation':
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor(dictionary=True)

            # Access can either be open or restricted with a modification of embargoed
            # so based on teh access search for the correct DUL and return it

            stmt = """
                SELECT d.*
                FROM data_use_limitation d
                LEFT JOIN cv_list c on d.dul_cv_list_id = c.id
                WHERE d.access = %(access)s 
                    and d.cohort_id = %(cohort_id)s 
                    and d.summary_files = %(summary_flag)s;
            """

            params = {'cohort_id' : cohort_id, "access" : access, "summary_flag" : summary_flag}
            self.log.debug(f"Executing stmt: {stmt} with params {params}")
            cursor.execute(stmt, params)

            row = cursor.fetchone()
            if row:
                return DataUseLimitation(self.log, row)
            return None

        except Exception as error:
            self.log.error("Failed in get_dul_from_cv_code() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def get_dul_from_cv_code_cohort_summary_flag(self, cv_code:str, cohort_id:int, summary_flag:int) -> 'DataUseLimitation':
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor(dictionary=True, buffered=True)

            stmt = """
                SELECT d.*
                FROM data_use_limitation d
                LEFT JOIN cv_list c on d.dul_cv_list_id = c.id
                WHERE c.term_id = %(term_id)s 
                    and d.cohort_id = %(cohort_id)s 
                    and d.summary_files = %(summary_flag)s;
            """

            params = {'cohort_id' : cohort_id, "term_id" : cv_code, "summary_flag" : summary_flag}
            self.log.debug(f"Executing stmt: {stmt} with params {params}")
            cursor.execute(stmt, params)

            row = cursor.fetchone()
            if row:
                return DataUseLimitation(self.log, row)
            return None

        except Exception as error:
            self.log.error("Failed in get_dul_from_cv_code() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                cursor.close()
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error
    

    def check_attribute_restriction(self, cohort_id, attribute_name, duo_code, access=None) -> 'DataUseLimitation':
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor(dictionary=True, buffered=True)
            stmt = """
                SELECT t3.id, t3.cohort_id, t3.access, t3.dul_cv_list_id, t3.summary_files, t3.date_added
                FROM
                    (SELECT t2.id, t2.cohort_id, t2.access, t2.summary_files, t2.date_added, t2.dul_cv_list_id, attributes_id
                    FROM attribute_has_data_use_limitation as t1
                    JOIN 
                        data_use_limitation as t2
                    ON t1.data_use_limitation_id = t2.id 
                    """ 
            if access:
                stmt += " WHERE t2.summary_files = 0 and t2.cohort_id = %(cohort_id)s and access = %(access)s) as t3 "
            else:
                stmt += " WHERE t2.summary_files = 0 and t2.cohort_id = %(cohort_id)s) as t3 "
            stmt += """
                JOIN
                    attributes as t4
                    ON t3.attributes_id = t4.id
                JOIN
                    cv_list as t5
                    ON t5.id = t3.dul_cv_list_id
                WHERE t4.attr_name = %(attribute_name)s and t5.term_id = %(duo_code)s
            """

            params = {'cohort_id' : cohort_id, "attribute_name" : attribute_name, "duo_code" : duo_code, "access" : access}
            self.log.debug(f"Executing stmt: {stmt} with params {params}")
            cursor.execute(stmt, params)

            row = cursor.fetchone()
            if row:
                d = DataUseLimitation(self.log, row)
                return d
            return None

        except Exception as error:
            self.log.error("Failed in check_attribute_restriction() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                cursor.close()
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def check_dul_has_attribute(self, dul_id, attribute_name):
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor(dictionary=True, buffered=True)
            stmt = """
                SELECT 1 
                FROM attribute_has_data_use_limitation as adul
                JOIN data_use_limitation dul ON dul.id = adul.data_use_limitation_id
                JOIN attributes as a ON a.id = adul.attributes_id
                WHERE attr_name = %(attribute_name)s and dul.id = %(dul_id)s;
            """

            params = {'dul_id' : dul_id, "attribute_name" : attribute_name}
            self.log.debug(f"Executing stmt: {stmt} with params {params}")
            cursor.execute(stmt, params)

            row = cursor.fetchone()
            if row:
                return True
            else:
                return False

        except Exception as error:
            self.log.error("Failed in check_dul_has_attribute() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                # cursor.close()
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error
