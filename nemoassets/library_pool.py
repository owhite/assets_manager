from base import Base
import sys

class LibraryPool(Base):   

    TABLE = "library_pool"

    # Indicates which fields are required for this table
    # Used for select and insert statements and validation
    FIELDS = {"library_pool_id" : True, "date_added" : False}
    
    # These are the fields allowed for use in the constructor
    ATTRS = ["id"] + list(FIELDS.keys())

    def __init__(self, log=None, params={}):   
        super().__init__(log, LibraryPool, params)

    def get_library_pool(self, params:dict):
        return self._get_record(params)
        
    def get_library_pools(self, params={}):
        return self._get_records(params)

    def add_library_pool(self, pool={}):
        mysql_cnx = None

        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt, data = self._build_insert_stmt(pool)
            self.log.debug(f"Executing stmt {stmt} with values {data}")
            cursor.execute(stmt, data)

            # Get the ID of the sample just created to add records to related tables
            pool_id = cursor.lastrowid
            self.log.debug(f"Library pool ID: {pool_id}")
            
            mysql_cnx.commit()

        except Exception as error:
            mysql_cnx.rollback()
            self.log.error("Failed in add_library_pool() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error
