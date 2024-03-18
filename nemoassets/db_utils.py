from typing import Any
import mysql.connector
import sys
import config
import transaction_manager


conf = config.Config.get_config()

###############################################################################
# These are wrapper functions to expose transaction_manager functions to the invoking (__main__) program
# Doing it this way because we don't want to import transaction_manager here and in __main__ where
# it would cause two instances of the transaction_manager module to be instantiated. 
# Instead, we're relying on the fact that a singly-imported python module is a singleton
###############################################################################
def begin_txn():
    # print("Beginning transaction")
    transaction_manager.begin_txn()

def commit_txn():
    # print("Committing transaction")
    transaction_manager.commit_txn()

def rollback_txn():
    # print("Rolling back transaction")
    transaction_manager.rollback_txn()

def set_connector(cnx):
    transaction_manager.set_connector(cnx)

def get_connector():
    # If we're in a transaction state, then re-use the connector. Otherwise, return None 
    # so that a new connector will be created. If we didn't do this, then when we're not 
    # in a transaction, the sample loader might call get_attribute_id_by_name(attr['attr_name'])
    # while building the associations. That call would invoke the close function and the 
    # singleton connector would be closed in the middle. As a result, it would be passing
    # around a dead cursor object.
    if transaction_manager.is_transaction_state():
        return transaction_manager.get_connector()
    return None

class SysOutLog():
    def debug(self, *args, **kwargs):
        print(args)
    def info(self, *args, **kwargs):
        print(args)
    def error(self, *args, **kwargs):
        print(args)
    def warning(self, *args, **kwargs):
        print(args)
    def exception(self, *args, **kwargs):
        print(args)

class StubLog():
    def debug(self, *args, **kwargs):
        pass
    def info(self, *args, **kwargs):
        pass
    def error(self, *args, **kwargs):
        pass
    def warning(self, *args, **kwargs):
        pass
    def exception(self, *args, **kwargs):
        pass

        
# This class is used to wrap the MySQLConnection object and override the
# commit function. It decides if it's in the middle of a transaction as
# started by the invoking (__main__) program and skips the commit; only
# doing so when the commit_transaction function is called
class CustomConnector():

    global transaction_state
    transaction_state = False

    def  __init__(self, base_obj, log, *args, **kwargs):
        self.base_obj = base_obj
        self.log = log
        self.transaction_state = False

    def commit_transaction(self):
        self.log.debug("Committing txn")
        self.base_obj.commit()
        transaction_manager.set_transaction_state(False)
        self.close_connection()

    def rollback_transaction(self):
        self.log.debug("Rollback txn")
        self.base_obj.rollback()
        transaction_manager.set_transaction_state(False)
        self.close_connection()
    
    def close_connection(self):
        if transaction_manager.is_transaction_state():
            self.log.debug("Skipping the close. Inside a txn")
        else:
            self.log.debug("Handling the close.")
            self.base_obj.cursor().close()
            self.base_obj.close()


    def commit(self):
        if transaction_manager.is_transaction_state():
            self.log.debug("Skipping the commit. Inside a txn")
        else:
            self.log.debug("Handling the commit.")
            self.base_obj.commit()

    def __getattr__(self, name):
        if name == "transaction_state" or name == "log":
            return object.__getattribute__(self, name)
        return object.__getattribute__(self.base_obj, name)


class Db:

    def __init__(self, log=None):
        if log:
            self.log = log
        else:
            self.log = StubLog()

    def get_db_connection(self):
        
        cnx = get_connector()

        if not cnx or not cnx.is_connected(): 
            self.log.debug("Connecting to assets database.")
            
            mysql_cnx = mysql.connector.connect(
                user=conf['database']['user'],
                password=conf['database']['passwd'],
                host=conf['database']['host'],
                database=conf['database']['db']
            )
            
            self.log.debug(f"Connected. Connection is {mysql_cnx}")
            mysql_cnx.autocommit = False
            custom_connector = CustomConnector(mysql_cnx, self.log)
            set_connector(custom_connector)
            self.log.debug("Connected.")
            return custom_connector
        
        # Re-using connector
        return cnx

    def close_connection(self, cnx):
        try:
            if(cnx is not None and cnx.is_connected()):
                cnx.close_connection()
        except Exception:
            self.log.error("Failed to close database connection. Squashing this exception.")
 
    def _get_id_from_unique_value(self, table_name, field, value):

        self.log.debug(f"Retrieving {table_name} {field} {value}")
        mysql_cnx = None
        try:
            mysql_cnx = self.get_db_connection()
            cursor = mysql_cnx.cursor(dictionary=True)

            stmt = f""" SELECT id
                        FROM {table_name}
                        WHERE {field} = %({field})s
                    """

            self.log.debug(f"Executing stmt {stmt} with params: {field} = {value}")
            cursor.execute(stmt, {field : value})

            results = []
            for row in cursor.fetchall():
                results.append(row)

            # self.log.debug("Returning " + str(len(results)) + " results")
            # return results

            # row = cursor.fetchone()
            if results:
                if len(results) == 1:
                    self.log.debug(f"Returning {table_name} ID: {results[0]['id']}")
                    return results[0]['id']
                else:
                    raise ValueError(f"Retrieved multiple records using {field} from {table_name}. Please use a field that has a unique constraint.")
            return None

        except Exception as error:
            self.log.error("Failed in _get_id_from_unique_value() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.close_connection(mysql_cnx)
            except Exception as error:
                raise error
            
 
    def _get_field_value_for_id(self, table_name, field, id):

        self.log.debug(f"Retrieving {table_name} {field}")
        mysql_cnx = None
        try:
            mysql_cnx = self.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = f""" SELECT {field}
                        FROM {table_name}
                        WHERE id = %(id)s
                    """

            self.log.debug(f"Executing stmt {stmt} with params: {field} = {id}")
            cursor.execute(stmt, {'id' : id})

            row = cursor.fetchone()
            if row:
                self.log.debug(f"Returning {table_name} ID: {row[0]}")
                return row[0]
            else:
                return None

        except Exception as error:
            self.log.error("Failed in _get_field_value_for_id() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.close_connection(mysql_cnx)
            except Exception as error:
                raise error
            
    def get_anatomy_id_by_short_name(self, anatomy):
        """
        Return the database anatomy ID for the anatomy.

        :param anatomy: The anatomy short name such as pre-frontal cortex, motor cortex
        :return: Internal database anatomyID
        """
        return self._get_id_from_unique_value("anatomy", "short_name", anatomy)

    def get_anatomy_id_by_cv_id(self, cv_id):
        """
        Return the database anatomy ID for the anatomy.

        :param cv_id
        :return: Internal database anatomy ID
        """
        return self._get_id_from_unique_value("anatomy", "cv_list_id", cv_id)
    
    def get_taxonomy_id(self, taxonomy):
        """
        Return the database taxonomy ID for the taxonomy.

        :param taxonomy: The common taxonomy name such as human, or mouse
        :return: Internal database taxonmy ID
        """
        return self._get_id_from_unique_value("taxonomy", "name", taxonomy)
    
    def get_modality_id(self, modality):
        """
        Return the database modality ID for the modality.

        :param modality: The modality name
        :return: Internal database taxonomy ID
        """
        return self._get_id_from_unique_value("modality", "name", modality)

    def get_assay_id(self, assay):
        """
        Return the database assay ID for the assay.

        :param assay: The assay name
        :return: Internal database assay ID
        """
        return self._get_id_from_unique_value("assay", "name", assay)
        
    
    def get_technique_id(self, technique):
        """
        Return the database technique ID for the technique.

        :param technique: The technique name
        :return: Internal database modality ID
        """
        return self._get_id_from_unique_value("technique", "name", technique)

    def get_technique_id_by_short_name(self, technique):
        """
        Return the database technique ID for the technique.

        :param technique: The technique short name
        :return: Internal database technique ID
        """
        return self._get_id_from_unique_value("technique", "short_name", technique)
    
    def get_attribute_id_by_name(self, attribute):
        return self._get_id_from_unique_value("attributes", "attr_name", attribute)

    def get_lab_name_by_id(self, id):
        """
        Return the laboratory name for the laboratory ID.

        :param id: Internal database laboratory ID.
        :return: The laboratory name.
        """
        return self._get_field_value_for_id("lab", "lab_name", id)

    def get_lab_id_by_name(self, lab):
        """
        Return the laboratory ID for the laboratory name.

        :param lab: The laboratory name.
        :return: Internal database laboratory ID.
        """
        return self._get_id_from_unique_value("lab", "lab_name", lab)

    def get_ins_cert_by_id(self, id):
        """
        Return the institutional certificate asset identifier for db ID.

        :param id: Internal institutional certificate ID.
        :return: The institutional certificate asset identifier.
        """
        return self._get_field_value_for_id("ins_cert", "ic_id", id)

    def get_ins_cert_id_by_asset_identifier(self, ic_id):
        """
        Return the institutional certificate ID for institutional certificate asset identifier.

        :param name: The institutional certificate asset identifier.
        :return: Internal database institutional certificate ID.
        """
        return self._get_id_from_unique_value("ins_cert", "ic_id", ic_id)

    def get_contributor_name_by_id(self, id):
        """
        Return the contributor name for the specified database identifier.

        :param id: The internal database identifier.
        :return: The lead PI's name.
        """
        return self._get_field_value_for_id("contributor", "name", id)

    def get_contibutor_id_by_email(self, email):
        """
        Return the contributor ID for the contributor email address.

        :param email: The contributor email.
        :return: Internal database contributor ID.
        """
        return self._get_id_from_unique_value("contributor", "email", email)

    def get_contibutor_id_by_name(self, name):
        """
        Return the contributor ID for the contributor name in the 'first_name last_name' format.

        :param name: The contributor name.
        :return: Internal database contributor ID.
        """
        return self._get_id_from_unique_value("contributor", "name", name)

    def get_library_pool_id_by_name(self, name):
        """
        Return the library pool ID for the library pool.

        :param name: The library pool name.
        :return: Internal database library pool ID.
        """
        return self._get_id_from_unique_value("library_pool", "library_pool_id", name)

    def get_collection_id_by_short_name(self, short_name):
        """
        Return the collection ID for the collection short name

        :param name: The collection short_name
        :return: Internal database collection ID.
        """
        return self._get_id_from_unique_value("collection", "short_name", short_name)
