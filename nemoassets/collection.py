import mysql.connector
import time
import sys
import os
import traceback
import getpass
from datetime import datetime, timedelta, date
import config, db_utils, project, subject, file, sample, contributor

conf = config.Config.get_config()

class Collection:   
    """
    The collection class models a collection of objects of a particular type (subject, sample, file), or of sub-collections and has the following 
    properties.

        id: Unique numeric ID assigned by database - Integer
        col_id: Unique alphanumeric collection identifier - String (required)
        col_type: Collection type, i.e., files, samples, subjects - String (required)
        is_static: Flag to indicate if this is a static collection. Values can be 0 or 1 - Integer
        short_name: Collection short name - String (required)
        name: Long collection name - String 
        description: Collection description - String
        access: A string identifying if the collection has open or controlled access - String 
        license: A string identifying the license associated with this collection - String
        is_published: Flag to indicate if this collection is associated with a publication. Values can be 0 or 1 - Integer
        doi: A DataCite digital object identifier (DOI) if one is issued for this collection. - String
        submission_status: A controlled list of string indicating if the collection is growing. - String
        url_knowledgebase: URL for any associated knowledgebase entry - String
        url_protocol: URL for any associated protocol entry - String
        publication: Information about the publication associated with this collection - Dictionary
        entity_urls: URL associated with entity - Dictionary

        projects: A list of projects specified by short name, associated with this collection - List    
        attributes: A dictionary of key/value pairs of attributes that are associated with this collection - Dictionary    
        contributors: A list of contributors, specified by email addresses, associated with this collection - List    
        subjects: A list of subject_names associated with this collection - List    
        samples: A list of sample_names associated with this collection - List    
        files: A list of file_names associated with this collection - List    
        anatomies: A list of anatomies_names associated with this collection - List    
        taxonomies: A list of taxonomies, specified by common names, associated with this collection - List    
        modalities: A list of modalities, specified by term name such as epigenome, whole genome, transcriptome, multiome, associated with this collection - List    
        assays: A list of assays, specified by a controlled list that include transcriptome, chromatin, mythylome, etc., associated with this collection - List    
        techniques: A list of specific techniques, such as 10xMultiome_ATACseq, 10XChromium_3', etc., associated with this collection - List    
        data_use_limitations: A list of associated ids from the data_use_limitation table
        child_collections: A list of child collections associated with this collection if any - List    
        date_added: Date and time record was created. Assigned by the database - Datetime
    """

    def __init__(self, log, params={}):   
        self.log = log
        self.db = db_utils.Db(log)

        self._read_from_db = 0

        if "id" in params:
            self.id = params["id"]
        else:
            self.id = None

        if "col_type" in params:
            self.col_type = params["col_type"]
        else:
            self.col_type = None

        if "col_id" in params:
            self.col_id = params["col_id"]
        else:
            self.col_id = None

        if "is_static" in params:
            self.is_static = params["is_static"]
        else:
            self.is_static = None

        if "short_name" in params:
            self.short_name = params["short_name"]
        else:
            self.short_name = None

        if "name" in params:
            self.name = params["name"]
        else:
            self.name = None

        if "description" in params:
            self.description = params["description"]
        else:
            self.description = None

        if "access" in params:
            self.access = params["access"]
        else:
            self.access = None

        if "is_published" in params:
            self.is_published = params["is_published"]
        else:
            self.is_published = None

        if "license" in params:
            self.license = params["license"]
        else:
            self.license = None

        if "doi" in params:
            self.doi = params["doi"]
        else:
            self.doi = None

        if "submission_status" in params:
            self.submission_status = params["submission_status"]
        else:
            self.submission_status = None

        if "url_protocol" in params:
            self.url_protocol = params["url_protocol"]
        else:
            self.url_protocol = None

        if "url_knowledgebase" in params:
            self.url_knowledgebase = params["url_knowledgebase"]
        else:
            self.url_knowledgebase = None

        if "attributes" in params:
            self.attributes = params["attributes"]
        else:
            self.attributes = {}

        if "child_collections" in params:
            self.child_collections = params["child_collections"]
        else:
            self.child_collections = {}

        if "subjects" in params:
            self.subjects = params["subjects"]
        else:
            self.subjects = []

        if "files" in params:
            self.files = params["files"]
        else:
            self.files = []

        if "samples" in params:
            self.samples = params["samples"]
        else:
            self.samples = []

        if "contributors" in params:
            self.contributors = params["contributors"]
        else:
            self.contributors = []

        if "anatomies" in params:
            self.anatomies = params["anatomies"]
        else:
            self.anatomies = []

        if "taxonomies" in params:
            self.taxonomies = params["taxonomies"]
        else:
            self.taxonomies = []

        if "modalities" in params:
            self.modalities = params["modalities"]
        else:
            self.modalities = []

        if "assays" in params:
            self.assays = params["assays"]
        else:
            self.assays = []

        if "techniques" in params:
            self.techniques = params["techniques"]
        else:
            self.techniques = []

        if "projects" in params:
            self.projects = params["projects"]
        else:
            self.projects = []

        if "publication" in params:
            self.publication = params["publication"]
        else:
            self.publication = None
            
        if "entity_urls" in params:
            self.entity_urls = params["entity_urls"]
        else:
            self.entity_urls = None
        
        if "data_use_limitations" in params:
            self._data_use_limitations = params["data_use_limitations"]
        else:
            self._data_use_limitations = None

        if "date_added" in params:
            self.date_added = params["date_added"]
        else:
            self.date_added = None


    @property
    def date_added(self):
        return self._date_added

    @date_added.setter
    def date_added(self, value):
        if value:
            if isinstance(value, str):
                self._date_added = date.fromisoformat(value)
            else:
                self._date_added = value
        else:
            self._date_added = value

    @property
    def attributes(self):
        if self._read_from_db:
            self.log.debug("Reading attributes associated with this collection from DB.")

            try:
                self._attributes = self._read_attribute_associations(self.id)
            except Exception as error:
                self.log.error("Failed in _read_attribute_associations() {}".format(error), exc_info=sys.exc_info())
                raise error

        return self._attributes

    @attributes.setter
    def attributes(self, value):
        self._attributes = value

    @property
    def child_collections(self):
        if self._read_from_db:
            self.log.debug("Reading child collections associated with this collection from DB.")

            try:
                self._child_collections = self._read_child_collection_associations(self.id)
            except Exception as error:
                self.log.error("Failed in _read_child_collection_associations() {}".format(error), exc_info=sys.exc_info())
                raise error

        return self._child_collections

    @child_collections.setter
    def child_collections(self, value):
        self._child_collections = value

    @property
    def subjects(self):
        if self._read_from_db:
            self.log.debug("Reading subjects associated with this collection from DB.")

            try:
                self._subjects = self._read_subject_associations(self.id)
            except Exception as error:
                self.log.error("Failed in _read_subject_associations() {}".format(error), exc_info=sys.exc_info())
                raise error

        return self._subjects

    @subjects.setter
    def subjects(self, value):
        self._subjects = value

    @property
    def files(self):
        if self._read_from_db:
            self.log.debug("Reading files associated with this collection from DB.")

            try:
                self._files = self._read_file_associations(self.id)
            except Exception as error:
                self.log.error("Failed in _read_file_associations() {}".format(error), exc_info=sys.exc_info())
                raise error

        return self._files

    @files.setter
    def files(self, value):
        self._files = value

    @property
    def samples(self):
        if self._read_from_db:
            self.log.debug("Reading samples associated with this collection from DB.")

            try:
                self._samples = self._read_sample_associations(self.id)
            except Exception as error:
                self.log.error("Failed in _read_sample_associations() {}".format(error), exc_info=sys.exc_info())
                raise error

        return self._samples

    @samples.setter
    def samples(self, value):
        self._samples = value

    @property
    def contributors(self):
        if self._read_from_db:
            self.log.debug("Reading contributors associated with this collection from DB.")

            try:
                self._contributors = self._read_contributor_associations(self.id)
            except Exception as error:
                self.log.error("Failed in _read_contributor_associations() {}".format(error), exc_info=sys.exc_info())
                raise error

        return self._contributors

    @contributors.setter
    def contributors(self, value):
        self._contributors = value

    @property
    def anatomies(self):
        if self._read_from_db:
            self.log.debug("Reading anatomies associated with this collection from DB.")

            try:
                self._anatomies = self._read_anatomy_associations(self.id)
            except Exception as error:
                self.log.error("Failed in _read_anatomy_associations() {}".format(error), exc_info=sys.exc_info())
                raise error

        return self._anatomies

    @anatomies.setter
    def anatomies(self, value):
        self._anatomies = value

    @property
    def modalities(self):
        if self._read_from_db:
            self.log.debug("Reading modalities associated with this collection from DB.")

            try:
                self._modalities = self._read_modality_associations(self.id)
            except Exception as error:
                self.log.error("Failed in _read_modality_associations() {}".format(error), exc_info=sys.exc_info())
                raise error

        return self._modalities

    @modalities.setter
    def modalities(self, value):
        self._modalities = value

    @property
    def assays(self):
        if self._read_from_db:
            self.log.debug("Reading assays associated with this collection from DB.")

            try:
                self._assays = self._read_assay_associations(self.id)
            except Exception as error:
                self.log.error("Failed in _read_assay_associations() {}".format(error), exc_info=sys.exc_info())
                raise error

        return self._assays

    @assays.setter
    def assays(self, value):
        self._assays = value

    @property
    def techniques(self):
        if self._read_from_db:
            self.log.debug("Reading techniques associated with this collection from DB.")

            try:
                self._techniques = self._read_technique_associations(self.id)
            except Exception as error:
                self.log.error("Failed in _read_technique_associations() {}".format(error), exc_info=sys.exc_info())
                raise error

        return self._techniques

    @techniques.setter
    def techniques(self, value):
        self._techniques = value

    @property
    def taxonomies(self):
        self.log.debug(f"** _read_from_db is {self._read_from_db}")
        if self._read_from_db:
            self.log.debug("Reading taxonomies associated with this collection from DB.")

            try:
                self._taxonomies = self._read_taxonomy_associations(self.id)
            except Exception as error:
                self.log.error("Failed in _read_taxonomy_associations() {}".format(error), exc_info=sys.exc_info())
                raise error

        return self._taxonomies

    @taxonomies.setter
    def taxonomies(self, value):
        self._taxonomies = value

    @property
    def projects(self):
        if self._read_from_db:
            self.log.debug("Reading projects associated with this collection from DB.")

            try:
                self._projects = self._read_project_associations(self.id)
            except Exception as error:
                self.log.error("Failed in _read_project_associations() {}".format(error), exc_info=sys.exc_info())
                raise error

        return self._projects

    @projects.setter
    def projects(self, value):
        self._projects = value

    @property
    def publication(self):
        if self._read_from_db:
            self.log.debug("Reading publication associated with this collection from DB.")

            try:
                self._publication = self._read_publication(self.id)
            except Exception as error:
                self.log.error("Failed in _read_publication() {}".format(error), exc_info=sys.exc_info())
                raise error

        return self._publication

    @publication.setter
    def publication(self, value):
        self._publication = value
        
        
    @property
    def entity_urls(self):
        if self._read_from_db:
            self.log.debug("Reading entity urls associated with this collection from DB.")

            try:
                self._entity_urls = self._read_entity_urls(self.id)
            except Exception as error:
                self.log.error("Failed in _read_entity_urls() {}".format(error), exc_info=sys.exc_info())
                raise error

        return self._entity_urls

    @entity_urls.setter
    def entity_urls(self, value):
        self._entity_urls = value

    @property
    def data_use_limitations(self):
        if self._read_from_db:
            self.log.debug("Reading data_use_limitations associated with this collection from DB.")

            try:
                self.log.debug("Assigning...")
                self._data_use_limitations = self._read_dul_associations(self.id)
                self.log.debug("Assigned")
            except Exception as error:
                self.log.error("Failed in _read_dul_associations() {}".format(error), exc_info=sys.exc_info())
                raise error
        
        self.log.debug("Returning dul")
        return self._data_use_limitations

    @data_use_limitations.setter
    def data_use_limitations(self, value):
        self._data_use_limitations = value


    def __str__(self):

        mystr = f"ID: {self.id} Asset ID: {self.col_id} Short Name: {self.short_name} Name: {self.name}\n"
        mystr += f"Access: {self.access} License: {self.license} Published: {self.is_published} DOI: {self.doi}"
        return mystr

    def get_all_collections(self):
        """
        Retrieve all collections from the database. 

        :return: A list of collection objects initialized with the properties.
        """

        # This method implements lazy loading, so all associations are not loaded
        # on creation, but loaded as needed.

        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """ SELECT id, col_id, col_type, is_static, short_name, name, description, access, license,
                            is_published, doi, submission_status, url_protocol, url_knowledgebase, date_added
                        FROM collection
                    """

            self.log.debug("Executing stmt: " + stmt)
            cursor.execute(stmt)

            results = []
            for row in cursor.fetchall():
                col = {}
                col["id"] = row[0]
                col["col_id"] = row[1]
                col["col_type"] = row[2]
                col["is_static"] = row[3]
                col["short_name"] = row[4]
                col["name"] = row[5]
                col["description"] = row[6]
                col["access"] = row[7]
                col["license"] = row[8]
                col["is_published"] = row[9]
                col["doi"] = row[10]
                col["submission_status"] = row[11]
                col["url_protocol"] = row[12]
                col["url_knoweldgebase"] = row[13]
                col["date_added"] = row[14]

                results.append(Collection(self.log, col))

            self.log.debug("Returning " + str(len(results)) + " results")
            return results

        except Exception as error:
            self.log.error("Failed in get_all_collections() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def get_collection(self, col_id):
        """
        Read the collection from database using the asset identifier.

        :param short_name: Collection asset identifier
        :return: returns collection object
        """
        # This method implements lazy loading, so all associations are not loaded
        # on creation, but loaded as needed.

        mysql_cnx = None
        self.log.debug(f"Fetching by collection ID: {col_id}")
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """ SELECT id, col_id, col_type, is_static, short_name, name, description, access, license,
                            is_published, doi, submission_status, url_protocol, url_knowledgebase, date_added
                        FROM collection
                        WHERE col_id = %(col_id)s
                    """

            data = {'col_id': col_id}
            self.log.debug(f"Executing stmt {stmt} with data {data}")
            cursor.execute(stmt, data)

            row = cursor.fetchone()
            if row:
                col = {}
                col["id"] = row[0]
                col["col_id"] = row[1]
                col["col_type"] = row[2]
                col["is_static"] = row[3]
                col["short_name"] = row[4]
                col["name"] = row[5]
                col["description"] = row[6]
                col["access"] = row[7]
                col["license"] = row[8]
                col["is_published"] = row[9]
                col["doi"] = row[10]
                col["submission_status"] = row[11]
                col["url_protocol"] = row[12]
                col["url_knoweldgebase"] = row[13]
                col["date_added"] = row[14]

                new_col = Collection(self.log, col)
                new_col._read_from_db = 1
                return new_col 
            else:
                return None

        except Exception as error:
            self.log.error("Failed in get_collection() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def get_collection_by_short_name(self, short_name):
        """
        Read the collection from database using the short name.

        :param short_name: Collection short name
        :return: returns collection object
        """
        # This method implements lazy loading, so all associations are not loaded
        # on creation, but loaded as needed.

        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """ SELECT id, col_id, col_type, is_static, short_name, name, description, access, license,
                            is_published, doi, submission_status, url_protocol, url_knowledgebase, date_added
                        FROM collection
                        WHERE short_name = %(short_name)s
                    """

            data = {'short_name': short_name}
            self.log.debug(f"Executing stmt {stmt} with data {data}")
            cursor.execute(stmt, data)

            row = cursor.fetchone()
            if row:
                col = {}
                col["id"] = row[0]
                col["col_id"] = row[1]
                col["col_type"] = row[2]
                col["is_static"] = row[3]
                col["short_name"] = row[4]
                col["name"] = row[5]
                col["description"] = row[6]
                col["access"] = row[7]
                col["license"] = row[8]
                col["is_published"] = row[9]
                col["doi"] = row[10]
                col["submission_status"] = row[11]
                col["url_protocol"] = row[12]
                col["url_knoweldgebase"] = row[13]
                col["date_added"] = row[14]
                
                new_col = Collection(self.log, col)
                new_col._read_from_db = 1
                return new_col
            
            return None

        except Exception as error:
            self.log.error("Failed in get_collection_by_short_name() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error
    
    def add_collection(self, col ={}):
        """
        Creates a database entry for the collection whose properties are supplied as a dictinary.

        :param col: A dictionary of the properties of this collection with the following keys

            col_id: Unique alphanumeric collection identifier - String (required)
            col_type: Collection type, i.e., files, samples, subjects - String (required)
            is_static: Flag to indicate if this is a static collection. Values can be 0 or 1 - Integer
            short_name: Collection short name - String (required)
            name: Long collection name - String 
            description: Collection description - String
            access: A string identifying if the collection has open or controlled access - String 
            license: A string identifying the license associated with this collection - String 
            is_published: Flag to indicate if this collection is associated with a publication. Values can be 0 or 1 - Integer
            doi: A DataCite digital object identifier (DOI) if one is issued for this collection. - String
            submission_status: A controlled list of string indicating if the collection is growing. - String
            url_knowledgebase: URL for any associated knowledgebase entry - String
            url_protocol: URL for any associated protocol entry - String
            publication: Information about the publication associated with this collection - Dictionary
            entity_urls: URL associated with entity - String

            projects: A list of projects specified by short name, associated with this collection - List    
            attributes: A dictionary of key/value pairs of attributes that are associated with this collection - Dictionary    
            contributors: A list of contributors, specified by email addresses, associated with this collection - List    
            subjects: A list of subject_names associated with this collection - List    
            samples: A list of sample_names associated with this collection - List    
            files: * DEPRECATED: file_name not unique * A list of file_names associated with this collection - List    
            anatomies: A list of anatomies_names associated with this collection - List    
            taxonomies: A list of taxonomies, specified by common names, associated with this collection - List    
            modalities: A list of modalities, specified by term name such as epigenome, whole genome, transcriptome, multiome, associated with this collection - List    
            assays: A list of assays, specified by a controlled list that include transcriptome, chromatin, mythylome, etc., associated with this collection - List    
            techniques: A list of specific techniques, such as 10xMultiome_ATACseq, 10XChromium_3', etc., associated with this collection - List    
            child_collections: A list of child collections associated with this collection if any - List    

        :return: No return value
        """
        mysql_cnx = None

        # Verify that all required fields are specified, else raise exception
        if ("col_id" not in col  or "col_type" not in col or "is_static" not in col or "short_name" not in col) :
            raise Exception("Missing one of the required parameters: col_id, col_type, is_static, short_name")
        try:

            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()

            # Build the query with required fields
            ins_stmt = "INSERT INTO collection (col_id, col_type, is_static, short_name"
            val_stmt = "VALUES(%s, %s, %s, %s"

            data = (col['col_id'], col['col_type'], col['is_static'], col['short_name'])

            # add to statement with optional fields
            if 'name' in col:
                ins_stmt = ins_stmt + ", name"
                val_stmt = val_stmt + ", %s"
                data = (*data, col['name'])

            if 'description' in col:
                ins_stmt = ins_stmt + ", description"
                val_stmt = val_stmt + ", %s"
                data = (*data, col['description'])

            if 'access' in col:
                ins_stmt = ins_stmt + ", access"
                val_stmt = val_stmt + ", %s"
                data = (*data, col['access'])

            if 'is_published' in col:
                ins_stmt = ins_stmt + ", is_published"
                val_stmt = val_stmt + ", %s"
                data = (*data, col['is_published'])

            if 'license' in col:
                ins_stmt = ins_stmt + ", license"
                val_stmt = val_stmt + ", %s"
                data = (*data, col['license'])

            if 'doi' in col:
                ins_stmt = ins_stmt + ", doi"
                val_stmt = val_stmt + ", %s"
                data = (*data, col['doi'])

            if 'submission_status' in col:
                ins_stmt = ins_stmt + ", submission_status"
                val_stmt = val_stmt + ", %s"
                data = (*data, col['submission_status'])

            if 'url_protocol' in col:
                ins_stmt = ins_stmt + ", url_protocol"
                val_stmt = val_stmt + ", %s"
                data = (*data, col['url_protocol'])

            if 'url_knowledgebase' in col:
                ins_stmt = ins_stmt + ", url_knowledgebase"
                val_stmt = val_stmt + ", %s"
                data = (*data, col['url_knowledgebase'])

            # Close the SQL statement
            stmt = ins_stmt + ") " + val_stmt + ")"

            self.log.debug(f"Executing stmt {stmt} with data {data}")
            cursor.execute(stmt, data)

            # Get the ID of the collection just created to add records to related tables
            collection_id = cursor.lastrowid
            self.log.debug(f"Collection ID: {collection_id}")

            # Add collection attributes if they have been specified
            if 'attributes' in col:
                self.log.debug("Adding attribute associations. " + str(col['attributes']))
                self._create_attribute_associations(collection_id, col['attributes'], cursor)

            # If there are child collectionss associated with this collection create associations
            if 'child_collections' in col:
                self.log.debug("Adding child collections associations. " + str(col['child_collections']))
                self._create_child_collection_associations(collection_id, col['child_collections'], cursor)

            # If there are subjects associated with this collection create associations
            if 'subjects' in col:
                self.log.debug("Adding subject associations. " + str(col['subjects']))
                self._create_subject_associations(collection_id, col['subjects'], cursor)
                    
            # If there are files associated with this collection create associations
            if 'files' in col:
                self.log.debug("Adding file associations. " + str(col['files']))
                self._create_file_associations(collection_id, col['files'], cursor)
                    
            # If there are samples associated with this collection create associations
            if 'samples' in col:
                self.log.debug("Adding sample associations. " + str(col['samples']))
                self._create_sample_associations(collection_id, col['samples'], cursor)
                    
            # If there are contributors associated with this collection create associations
            if 'contributors' in col:
                self.log.debug("Adding contributor associations. " + str(col['contributors']))
                self._create_contributor_associations(collection_id, col['contributors'], cursor)
                    
            # If there are anatomies associated with this collection create associations
            if 'anatomies' in col:
                self.log.debug("Adding anatomy associations. " + str(col['anatomies']))
                self._create_anatomy_associations(collection_id, col['anatomies'], cursor)
                    
            # If there are modalities associated with this collection create associations
            if 'modalities' in col:
                self.log.debug("Adding modality associations. " + str(col['modalities']))
                self._create_modality_associations(collection_id, col['modalities'], cursor)
                    
            # If there are assays associated with this collection create associations
            if 'assays' in col:
                self.log.debug("Adding assay associations. " + str(col['assays']))
                self._create_assay_associations(collection_id, col['assays'], cursor)
                    
            # If there are techniques associated with this collection create associations
            if 'techniques' in col:
                self.log.debug("Adding techniques associations. " + str(col['techniques']))
                self._create_technique_associations(collection_id, col['techniques'], cursor)
                    
            # If there are taxonomies associated with this collection create associations
            if 'taxonomies' in col:
                self.log.debug("Adding taxonomy associations. " + str(col['taxonomies']))
                self._create_taxonomy_associations(collection_id, col['taxonomies'], cursor)
                    
            # If there are projects associated with this collection create associations
            if 'projects' in col:
                self.log.debug("Adding projects associations. " + str(col['projects']))
                self._create_project_associations(collection_id, col['projects'], cursor)
                
            # If there is publication associated with this collection create associations
            if 'publication' in col:
                self.log.debug("Adding publication. " + str(col['publication']))
                self._add_publication(collection_id, col['publication'], cursor)
                
            # If there are entity urls associated with this collection create associations
            if 'entity_urls' in col:
                self.log.debug("Adding entity urls. " + str(col['entity_urls']))
                self._add_entity_urls(collection_id, col['entity_urls'], cursor)
                
            # If there are data_use_limitations associated with this collection create associations                
            if 'data_use_limitations' in col:
                self.log.debug("Adding entity urls. " + str(col['data_use_limitations']))
                self._create_dul_associations(collection_id, col['data_use_limitations'], cursor)

            mysql_cnx.commit()
            return self.get_collection(col['col_id'])
        
        except Exception as error:
            mysql_cnx.rollback()
            self.log.error("Failed in add_project() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def create_attribute_associations(self, collection_id, attributes_list):
        """
        Create attribute associations for this collection. 

        :param collection_id: The internal database ID
        :param attributes_list: The list of attributes as a dictionary
        :return: returns nothing
        """
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            self._create_attribute_associations(collection_id, attributes_list, cursor) 
            mysql_cnx.commit()
        except Exception as error:
            mysql_cnx.rollback()
            raise error                 
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def _read_attribute_associations(self, collection_id):
        """
        Read attribute associations for this collection. 

        :param collection_id: The internal database ID
        :return attributes_list: returns the attributes as a dictionary.
        """
        self.log.debug(f"Reading attributes associated with collection ID: {collection_id}")
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """SELECT name, value
                        FROM collection_attributes 
                        WHERE collection_id = %s
                    """
            data = (collection_id, )
            self.log.debug(f"Executing stmt {stmt} with data {data}")
            cursor.execute(stmt, data)
            attributes_list = {}
            for row in cursor.fetchall():
                name = row[0]
                value = row[1]
                attributes_list[name] = value

            return attributes_list

        except Exception as error:
            self.log.error("Failed in _read_attribute_associations() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                if mysql_cnx:
                    self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def _create_attribute_associations(self, collection_id, attributes_list, cursor):
        """
        Create attribute associations for this collection. This method is called in the context of an existing 
        transaction, so it does not commit the changes. The calling method should ensure changes are committed.

        :param collection_id: The internal database ID
        :param attributes_list: The list of attributes as a dictionary
        :param cursor: An cursor object on the connection.
        :return: returns nothing
        """

        try:

            self.log.debug(str(attributes_list))
            for key, value in attributes_list.items():
                stmt = """INSERT INTO collection_attributes (collection_id, name, value)
                        VALUES (%s, %s, %s)
                        """
                data = (collection_id, key, value)
                self.log.debug(f"Executing stmt: {stmt} with collection_id {collection_id}, name {key}, value {value}")
                cursor.execute(stmt, data)

        except Exception as error:
            self.log.error("Failed in _create_attribute_associations() {}".format(error), exc_info=sys.exc_info())
            raise error

    def create_child_collection_associations(self, collection_id, child_collections_list):
        """
        Create child collections associations for this collection. 

        :param collection_id: The internal database ID
        :param child_collections_list: The list of child collecti asset IDs
        :return: returns nothing
        """
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            self._create_child_collection_associations(collection_id, child_collections_list, cursor) 
            mysql_cnx.commit()
        except Exception as error:
            mysql_cnx.rollback()
            raise error                 
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def _create_child_collection_associations(self, collection_id, child_collections_list, cursor):
        """
        Create child collections associations for this collection. This method is called in the context of an existing 
        transaction, so it does not commit the changes. The calling method should ensure changes are committed.

        :param collection_id: The internal database ID
        :param child_collections_list: The list of child collection asset IDs
        :param cursor: An cursor object on the connection.
        :return: returns nothing
        """

        try:

            self.log.debug(str(child_collections_list))
            for col_id in child_collections_list:
                child_collection_id = self.get_collection(col_id)
                stmt = """INSERT INTO collection_has_collection (parent_collection_id, child_collection_id);
                        VALUES (%s, %s)
                        """
                data = (collection_id, child_collection_id)
                self.log.debug(f"Executing stmt: {stmt} with values {data}")
                cursor.execute(stmt, data)

        except Exception as error:
            self.log.error("Failed in _create_child_collection_associations() {}".format(error), exc_info=sys.exc_info())
            raise error

    def _read_child_collection_associations(self, collection_id):
        """
        Read child collections associations for this collection. 

        :param collection_id: The internal database ID
        :return child_collections_list: returns the child collections as a list of asset IDs
        """
        self.log.debug(f"Reading child collections associated with collection ID: {collection_id}")
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """SELECT col_id
                        FROM collection c 
                        JOIN collection_has_collection cac ON cac.child_collection_id = c.id
                        WHERE cac.parent_collection_id = %s
                    """
            data = (collection_id, )
            self.log.debug(f"Executing stmt {stmt} with data {data}")
            cursor.execute(stmt, data)
            child_collections_list = []
            for row in cursor.fetchall():
                child_collections_list.append(row[0])

            return child_collections_list

        except Exception as error:
            self.log.error("Failed in _read_child_collection_associations() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                if mysql_cnx:
                    self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error


    def create_subject_associations(self, collection_id, subjects_list):
        """
        Create subject associations for this collection. 

        :param collection_id: The internal database ID
        :param subjects_list: The list of source subject IDs
        :return: returns nothing
        """
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            self._create_subject_associations(collection_id, subjects_list, cursor) 
            mysql_cnx.commit()
        except Exception as error:
            mysql_cnx.rollback()
            raise error                 
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def _read_subject_associations(self, collection_id):
        """
        Read subject associations for this collection. 

        :param collection_id: The internal database ID
        :return subjects_list: returns the subjects as a lst of source sibject IDs if any associated with this collection, else a null object
        """
        self.log.debug(f"Reading subjects associated with collection ID: {collection_id}")
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """SELECT source_subject_id
                        FROM collection c 
                            JOIN subject_assoc_collection sac ON c.id = sac.collection_id 
                            JOIN subject s ON sac.subject_id = s.id
                        WHERE collection_id = %s
                    """
            data = (collection_id, )
            self.log.debug(f"Executing stmt {stmt} with data {data}")
            cursor.execute(stmt, data)
            subjects_list = []
            for row in cursor.fetchall():
                source_subject_id = row[0]
                subjects_list.append(source_subject_id)

            return subjects_list

        except Exception as error:
            self.log.error("Failed in _read_subject_associations() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                if mysql_cnx:
                    self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def _create_subject_associations(self, collection_id, subjects_list, cursor):
        """
        Create subject associations for this collection. This method is called in the context of an existing 
        transaction, so it does not commit the changes. The calling method should ensure changes are committed.

        :param collection_id: The internal database ID
        :param subjects_list: The list of source subject IDs
        :param cursor: An cursor object on the connection.
        :return: returns nothing
        """

        try:

            self.log.debug(subjects_list)
            s = subject.Subject(self.log)
            for source_subject_id in subjects_list:
                sub = s.get_subject_by_source_subject_id(source_subject_id)
                if not sub:
                    self.log.error(f"Cannot find subject with source subject ID : {source_subject_id}")
                    raise Exception(f"Cannot find subject with source subject ID : {source_subject_id}")
                subject_id = sub.id    
                stmt = """INSERT INTO subject_assoc_collection (collection_id, subject_id)
                                VALUES (%s, %s)
                            """
                data = (collection_id, subject_id)
                self.log.debug(f"Executing stmt {stmt} with data {data}")
                cursor.execute(stmt, data)

        except Exception as error:
            self.log.error("Failed in _create_subject_associations() {}".format(error), exc_info=sys.exc_info())
            raise error

    def create_file_associations(self, collection_id, files_list):
        """
        Create file associations for this collection. 

        :param collection_id: The internal database ID
        :param files_list: The list of source file IDs
        :return: returns nothing
        """
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            self._create_file_associations(collection_id, files_list, cursor) 
            mysql_cnx.commit()
        except Exception as error:
            mysql_cnx.rollback()
            raise error                 
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error


    def _read_file_associations(self, collection_id):
        """
        Read file associations for this collection. 

        :param collection_id: The internal database ID
        :return files_list: returns the files as a lst of source sibject IDs if any associated with this collection, else a null object
        """
        self.log.debug(f"Reading files associated with collection ID: {collection_id}")
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """SELECT file_name
                        FROM collection c 
                            JOIN file_in_collection fic ON c.id = fic.collection_id 
                            JOIN file f ON fic.file_id = f.id
                        WHERE collection_id = %s
                    """
            data = (collection_id, )
            self.log.debug(f"Executing stmt {stmt} with data {data}")
            cursor.execute(stmt, data)
            files_list = []
            for row in cursor.fetchall():
                file_name = row[0]
                files_list.append(file_name)

            return files_list

        except Exception as error:
            self.log.error("Failed in _read_file_associations() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                if mysql_cnx:
                    self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def _create_file_associations(self, collection_id, files_list, cursor):
        """
        ** TODO ** - File names are not unique on the file table so we probably want to change 
        this to take additional information to be certain we are linking the correct file. In the meantime,
        the get_file_by_filename() function and other functions that internally rely on base._get_record()
        will now raise an exception when more than one record is returned from the DB.

        Create file associations for this collection. This method is called in the context of an existing 
        transaction, so it does not commit the changes. The calling method should ensure changes are committed.

        :param collection_id: The internal database ID
        :param files_list: The list of source file IDs
        :param cursor: An cursor object on the connection.
        :return: returns nothing
        """

        try:

            self.log.debug(files_list)
            s = file.File(self.log)
            for filename in files_list:
                sub = s.get_file_by_filename(filename)
                if not sub:
                    self.log.error(f"Cannot find file with file: {filename}")
                    raise Exception(f"Cannot find file with file: {filename}")
                file_id = sub.id    
                stmt = """INSERT INTO file_in_collection (collection_id, file_id)
                                VALUES (%s, %s)
                            """
                data = (collection_id, file_id)
                self.log.debug(f"Executing stmt {stmt} with data {data}")
                cursor.execute(stmt, data)

        except Exception as error:
            self.log.error("Failed in _create_file_associations() {}".format(error), exc_info=sys.exc_info())
            raise error

    def create_sample_associations(self, collection_id, samples_list):
        """
        Create sample associations for this collection. 

        :param collection_id: The internal database ID
        :param samples_list: The list of source sample IDs
        :return: returns nothing
        """
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            self._create_sample_associations(collection_id, samples_list, cursor) 
            mysql_cnx.commit()
        except Exception as error:
            mysql_cnx.rollback()
            raise error                 
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def _create_sample_associations(self, collection_id, samples_list, cursor):
        """
        Create sample associations for this collection. This method is called in the context of an existing 
        transaction, so it does not commit the changes. The calling method should ensure changes are committed.

        :param collection_id: The internal database ID
        :param samples_list: The list of source sample IDs
        :param cursor: An cursor object on the connection.
        :return: returns nothing
        """

        try:

            self.log.debug(samples_list)
            s = sample.Sample(self.log)
            for source_sample_id in samples_list:
                sub = s.get_sample_by_sourceid(source_sample_id)
                if not sub:
                    self.log.error(f"Cannot find sample with sample: {source_sample_id}")
                    raise Exception(f"Cannot find sample with sample: {source_sample_id}")
                sample_id = sub.id    
                stmt = """INSERT INTO sample_assoc_collection (collection_id, sample_id)
                                VALUES (%s, %s)
                            """
                data = (collection_id, sample_id)
                self.log.debug(f"Executing stmt {stmt} with data {data}")
                cursor.execute(stmt, data)

        except Exception as error:
            self.log.error("Failed in _create_sample_associations() {}".format(error), exc_info=sys.exc_info())
            raise error

    def _read_sample_associations(self, collection_id):
        """
        Read sample associations for this collection. 

        :param collection_id: The internal database ID
        :return samples_list: returns the samples as a list of source sample IDs
        """
        self.log.debug(f"Reading samples associated with collection ID: {collection_id}")
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """SELECT source_sample_id
                        FROM collection c 
                            JOIN sample_assoc_collection sac ON c.id = sac.collection_id 
                            JOIN sample s ON sac.sample_id = s.id
                        WHERE collection_id = %s
                    """
            data = (collection_id, )
            self.log.debug(f"Executing stmt {stmt} with data {data}")
            cursor.execute(stmt, data)
            samples_list = []
            for row in cursor.fetchall():
                source_sample_id = row[0]
                samples_list.append(source_sample_id)

            return samples_list

        except Exception as error:
            self.log.error("Failed in _read_sample_associations() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                if mysql_cnx:
                    self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def create_contributor_associations(self, collection_id, contributors_list):
        """
        Create contributor associations for this collection. 

        :param collection_id: The internal database ID
        :param contributors_list: The list of source contributor emails
        :return: returns nothing
        """
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            self._create_contributor_associations(collection_id, contributors_list, cursor) 
            mysql_cnx.commit()
        except Exception as error:
            mysql_cnx.rollback()
            raise error                 
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def _create_contributor_associations(self, collection_id, contributors_list, cursor):
        """
        Create contributor associations for this collection. This method is called in the context of an existing 
        transaction, so it does not commit the changes. The calling method should ensure changes are committed.

        :param collection_id: The internal database ID
        :param contributors_list: The list of source contributor emails
        :param cursor: An cursor object on the connection.
        :return: returns nothing
        """

        try:

            self.log.debug(contributors_list)
            s = contributor.Contributor(self.log)
            for contributor_email in contributors_list:
                sub = s.get_contributor_by_email(contributor_email)
                if not sub:
                    self.log.error(f"Cannot find contributor with contributor: {contributor_email}")
                    raise Exception(f"Cannot find contributor with contributor: {contributor_email}")
                contributor_id = sub.id    
                stmt = """INSERT INTO collection_has_contributor (collection_id, contributor_id)
                                VALUES (%s, %s)
                            """
                data = (collection_id, contributor_id)
                self.log.debug(f"Executing stmt {stmt} with data {data}")
                cursor.execute(stmt, data)

        except Exception as error:
            self.log.error("Failed in _create_contributor_associations() {}".format(error), exc_info=sys.exc_info())
            raise error

    def _read_contributor_associations(self, collection_id):
        """
        Read contributor associations for this collection. 

        :param collection_id: The internal database ID
        :return contributors_list: returns the contributors as a list of emails
        """
        self.log.debug(f"Reading contributors associated with collection ID: {collection_id}")
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """SELECT email
                        FROM collection c 
                            JOIN collection_has_contributor chc ON c.id = chc.collection_id 
                            JOIN contributor co ON chc.contributor_id = co.id
                        WHERE collection_id = %s
                    """
            data = (collection_id, )
            self.log.debug(f"Executing stmt {stmt} with data {data}")
            cursor.execute(stmt, data)
            contributors_list = []
            for row in cursor.fetchall():
                source_contributor_id = row[0]
                contributors_list.append(source_contributor_id)

            return contributors_list

        except Exception as error:
            self.log.error("Failed in _read_contributor_associations() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                if mysql_cnx:
                    self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def create_anatomy_associations(self, collection_id, anatomies_list):
        """
        Create anatomy associations for this collection. 

        :param collection_id: The internal database ID
        :param anatomies_list: The list of source anatomy IDs
        :return: returns nothing
        """
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            self._create_anatomy_associations(collection_id, anatomies_list, cursor) 
            mysql_cnx.commit()
        except Exception as error:
            mysql_cnx.rollback()
            raise error                 
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def _create_anatomy_associations(self, collection_id, anatomies_list, cursor):
        """
        Create anatomy associations for this collection. This method is called in the context of an existing 
        transaction, so it does not commit the changes. The calling method should ensure changes are committed.

        :param collection_id: The internal database ID
        :param anatomies_list: The list of anatomy short names
        :param cursor: An cursor object on the connection.
        :return: returns nothing
        """

        try:

            self.log.debug(f"Adding anatomies {anatomies_list} to collection ID: {collection_id}")
            for anatomy_short_name in anatomies_list:
                anatomy_id = self.db.get_anatomy_id(anatomy_short_name)
                if not anatomy_id:
                    self.log.error(f"Cannot find anatomy with name {anatomy_short_name}")
                    raise Exception(f"Cannot find anatomy with name: {anatomy_short_name}")
                stmt = """INSERT INTO collection_assoc_anatomy (collection_id, anatomy_id)
                                VALUES (%s, %s)
                            """
                data = (collection_id, anatomy_id)
                self.log.debug(f"Executing stmt {stmt} with data {data}")
                cursor.execute(stmt, data)

        except Exception as error:
            self.log.error("Failed in _create_anatomy_associations() {}".format(error), exc_info=sys.exc_info())
            raise error

    def _read_anatomy_associations(self, collection_id):
        """
        Read anatomy associations for this collection. 

        :param collection_id: The internal database ID
        :return anatomies_list: returns the anatomies as a list of anatomy short names
        """
        self.log.debug(f"Reading anatomies associated with collection ID: {collection_id}")
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """SELECT a.short_name
                        FROM collection c 
                            JOIN collection_assoc_anatomy cac ON c.id = cac.collection_id 
                            JOIN anatomy a ON cac.anatomy_id = a.id
                        WHERE collection_id = %s
                    """
            data = (collection_id, )
            self.log.debug(f"Executing stmt {stmt} with data {data}")
            cursor.execute(stmt, data)
            anatomies_list = []
            for row in cursor.fetchall():
                short_name = row[0]
                anatomies_list.append(short_name)

            return anatomies_list

        except Exception as error:
            self.log.error("Failed in _read_anatomy_associations() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                if mysql_cnx:
                    self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def create_taxonomy_associations(self, collection_id, taxonomies_list):
        """
        Create taxonomy associations for this collection. 

        :param collection_id: The internal database ID
        :param taxonomies_list: The list of source taxonomy names
        :return: returns nothing
        """
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            self._create_taxonomy_associations(collection_id, taxonomies_list, cursor) 
            mysql_cnx.commit()
        except Exception as error:
            mysql_cnx.rollback()
            raise error                 
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def _create_taxonomy_associations(self, collection_id, taxonomies_list, cursor):
        """
        Create taxonomy associations for this collection. This method is called in the context of an existing 
        transaction, so it does not commit the changes. The calling method should ensure changes are committed.

        :param collection_id: The internal database ID
        :param taxonomies_list: The list of source taxonomy names
        :param cursor: An cursor object on the connection
        :return: returns nothing
        """

        try:
            # Insert the taxonomies associated with this collection
            self.log.debug(taxonomies_list)
            for taxon in taxonomies_list:
                self.log.debug(f"Processing taxonomy {taxon}")
                taxonomy_id = self.db.get_taxonomy_id(taxon)

                if not taxonomy_id:
                    self.log.error(f"Cannot find taxonomy with name: {taxon}")
                    raise Exception(f"Cannot find taxonomy with name: {taxon}")

                stmt = """INSERT INTO collection_assoc_species (collection_id, taxonomy_id)
                                VALUES (%s, %s)
                """
                data = (collection_id, taxonomy_id)
                self.log.debug(f"Executing stmt {stmt} with data {data}")
                cursor.execute(stmt, data)

        except Exception as error:
            self.log.error("Failed in _create_taxonomy_associations() {}".format(error), exc_info=sys.exc_info())
            raise error

    def _read_taxonomy_associations(self, collection_id):
        """
        Read taxonomy associations for this collection. 

        :param collection_id: The internal database ID
        :return anatomies_list: returns the taxonomies as a list of taxonomy names
        """
        self.log.debug(f"Reading taxonomies associated with collection ID: {collection_id}")
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """SELECT t.name
                        FROM collection c 
                            JOIN collection_assoc_species cas ON c.id = cas.collection_id 
                            JOIN taxonomy t ON cas.taxonomy_id = t.id
                        WHERE collection_id = %s
                    """
            data = (collection_id, )
            self.log.debug(f"Executing stmt {stmt} with data {data}")
            cursor.execute(stmt, data)
            taxonomies_list = []
            for row in cursor.fetchall():
                name = row[0]
                taxonomies_list.append(name)

            return taxonomies_list

        except Exception as error:
            self.log.error("Failed in _read_taxonomy_associations() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                if mysql_cnx:
                    self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def create_modality_associations(self, collection_id, modalities_list):
        """
        Create modality associations for this collection. 

        :param collection_id: The internal database ID
        :param modalities_list: The list of source modality names
        :return: returns nothing
        """
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            self._create_modality_associations(collection_id, modalities_list, cursor) 
            mysql_cnx.commit()
        except Exception as error:
            mysql_cnx.rollback()
            raise error                 
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def _create_modality_associations(self, collection_id, modalities_list, cursor):
        """
        Create modality associations for this collection. This method is called in the context of an existing 
        transaction, so it does not commit the changes. The calling method should ensure changes are committed.

        :param collection_id: The internal database ID
        :param modalities_list: The list of source modality names
        :param cursor: An cursor object on the connection
        :return: returns nothing
        """

        try:
            # Insert the modalities associated with this collection
            self.log.debug(modalities_list)
            for modality in modalities_list:
                self.log.debug(f"Processing modality {modality}")
                modality_id = self.db.get_modality_id(modality)

                if not modality_id:
                    self.log.error(f"Cannot find modality with name: {modality}")
                    raise Exception(f"Cannot find modality with name: {modality}")

                stmt = """INSERT INTO collection_assoc_modality (collection_id, modality_id)
                                VALUES (%s, %s)
                """
                data = (collection_id, modality_id)
                self.log.debug(f"Executing stmt {stmt} with data {data}")
                cursor.execute(stmt, data)

        except Exception as error:
            self.log.error("Failed in _create_modality_associations() {}".format(error), exc_info=sys.exc_info())
            raise error

    def _read_modality_associations(self, collection_id):
        """
        Read modality associations for this collection. 

        :param collection_id: The internal database ID
        :return anatomies_list: returns the modalities as a list of modality names
        """
        self.log.debug(f"Reading modalities associated with collection ID: {collection_id}")
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """SELECT m.name
                        FROM collection c 
                            JOIN collection_assoc_modality cam ON c.id = cam.collection_id 
                            JOIN modality m ON cam.modality_id = m.id
                        WHERE collection_id = %s
                    """
            data = (collection_id, )
            self.log.debug(f"Executing stmt {stmt} with data {data}")
            cursor.execute(stmt, data)
            modalities_list = []
            for row in cursor.fetchall():
                name = row[0]
                modalities_list.append(name)

            return modalities_list

        except Exception as error:
            self.log.error("Failed in _read_modality_associations() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                if mysql_cnx:
                    self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def create_dul_associations(self, collection_id, data_use_limitations):
        """
        Create data_use_limitation associations for this collection. 

        :param collection_id: The internal database ID
        :param data_use_limitations: The list of data_use_limitation IDs
        :return: returns nothing
        """
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            self._create_dul_associations(collection_id, data_use_limitations, cursor) 
            mysql_cnx.commit()
        except Exception as error:
            mysql_cnx.rollback()
            raise error                 
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def _create_dul_associations(self, collection_id, data_use_limitations, cursor):
        """
        Create data_use_limitation associations for this collection. This method is called in the context of an existing 
        transaction, so it does not commit the changes. The calling method should ensure changes are committed.

        :param collection_id: The internal database ID
        :param data_use_limitations: The list of data_use_limitation IDs
        :param cursor: An cursor object on the connection.
        :return: returns nothing
        """

        try:

            self.log.debug(f"Creating collection assocation to duls: {data_use_limitations}")
            s = sample.Sample(self.log)
            for dul_id in data_use_limitations:
                # sub = s.get_sample_by_sourceid(source_sample_id)
                # if not sub:
                    # self.log.error(f"Cannot find sample with sample: {source_sample_id}")
                    # raise Exception(f"Cannot find sample with sample: {source_sample_id}")
                # sample_id = sub.id    
                stmt = """INSERT INTO collection_has_data_use_condition (collection_id, data_use_condition_duc_id)
                                VALUES (%s, %s)
                            """
                data = (collection_id, dul_id)
                self.log.debug(f"Executing stmt {stmt} with data {data}")
                cursor.execute(stmt, data)

        except Exception as error:
            self.log.error("Failed in _create_sample_associations() {}".format(error), exc_info=sys.exc_info())
            raise error

    def _read_dul_associations(self, collection_id):
        """
        Read data_use_limitation associations for this collection. 

        :param collection_id: The internal database ID
        :return data_use_limitation list: returns the data_use_limitation associations as a list of ids from the data_use_limitation table
        """
        self.log.debug(f"Reading data_use_limitation associated with collection ID: {collection_id}")
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """
                SELECT dul.id
                FROM data_use_limitation dul, collection_has_data_use_condition cdul
                WHERE dul.id = cdul.data_use_condition_duc_id and collection_id = %s
            """
            data = (collection_id, )
            self.log.debug(f"Executing stmt {stmt} with data {data}")
            cursor.execute(stmt, data)
            dul_list = []
            for row in cursor.fetchall():
                id = row[0]
                dul_list.append(id)

            return dul_list

        except Exception as error:
            self.log.error("Failed in _read_dul_associations() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                if mysql_cnx:
                    self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error
            
    def create_assay_associations(self, collection_id, assays_list):
        """
        Create assay associations for this collection. 

        :param collection_id: The internal database ID
        :param assays_list: The list of source assay names
        :return: returns nothing
        """
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            self._create_assay_associations(collection_id, assays_list, cursor) 
            mysql_cnx.commit()
        except Exception as error:
            mysql_cnx.rollback()
            raise error                 
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def _create_assay_associations(self, collection_id, assays_list, cursor):
        """
        Create assay associations for this collection. This method is called in the context of an existing 
        transaction, so it does not commit the changes. The calling method should ensure changes are committed.

        :param collection_id: The internal database ID
        :param assays_list: The list of source assay names
        :param cursor: An cursor object on the connection
        :return: returns nothing
        """

        try:
            # Insert the assays associated with this collection
            self.log.debug(assays_list)
            for assay in assays_list:
                self.log.debug(f"Processing assay {assay}")
                assay_id = self.db.get_assay_id(assay)

                if not assay_id:
                    self.log.error(f"Cannot find assay with name: {assay}")
                    raise Exception(f"Cannot find assay with name: {assay}")

                stmt = """INSERT INTO collection_assoc_assay (collection_id, assay_id)
                                VALUES (%s, %s)
                """
                data = (collection_id, assay_id)
                self.log.debug(f"Executing stmt {stmt} with data {data}")
                cursor.execute(stmt, data)

        except Exception as error:
            self.log.error("Failed in _create_assay_associations() {}".format(error), exc_info=sys.exc_info())
            raise error

    def _read_assay_associations(self, collection_id):
        """
        Read assay associations for this collection. 

        :param collection_id: The internal database ID
        :return assays_list: returns the assays as a list of assay names
        """
        self.log.debug(f"Reading assays associated with collection ID: {collection_id}")
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """SELECT a.name
                        FROM collection c 
                            JOIN collection_assoc_assay caa ON c.id = caa.collection_id 
                            JOIN assay a ON caa.assay_id = a.id
                        WHERE collection_id = %s
                    """
            data = (collection_id, )
            self.log.debug(f"Executing stmt {stmt} with data {data}")
            cursor.execute(stmt, data)
            assays_list = []
            for row in cursor.fetchall():
                name = row[0]
                assays_list.append(name)

            return assays_list

        except Exception as error:
            self.log.error("Failed in _read_assay_associations() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                if mysql_cnx:
                    self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def create_technique_associations(self, collection_id, techniques_list):
        """
        Create technique associations for this collection. 

        :param collection_id: The internal database ID
        :param techniques_list: The list of source technique names
        :return: returns nothing
        """
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            self._create_technique_associations(collection_id, techniques_list, cursor) 
            mysql_cnx.commit()
        except Exception as error:
            mysql_cnx.rollback()
            raise error                 
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def _create_technique_associations(self, collection_id, techniques_list, cursor):
        """
        Create technique associations for this collection. This method is called in the context of an existing 
        transaction, so it does not commit the changes. The calling method should ensure changes are committed.

        :param collection_id: The internal database ID
        :param techniques_list: The list of source technique names
        :param cursor: An cursor object on the connection
        :return: returns nothing
        """

        try:
            # Insert the techniques associated with this collection
            self.log.debug(techniques_list)
            for technique in techniques_list:
                self.log.debug(f"Processing technique {technique}")
                technique_id = self.db.get_technique_id_by_short_name(technique)

                if not technique_id:
                    self.log.error(f"Cannot find technique with name: {technique}")
                    raise Exception(f"Cannot find technique with name: {technique}")

                stmt = """INSERT INTO collection_assoc_technique (collection_id, technique_id)
                                VALUES (%s, %s)
                """
                data = (collection_id, technique_id)
                self.log.debug(f"Executing stmt {stmt} with data {data}")
                cursor.execute(stmt, data)

        except Exception as error:
            self.log.error("Failed in _create_technique_associations() {}".format(error), exc_info=sys.exc_info())
            raise error

    def _read_technique_associations(self, collection_id):
        """
        Read technique associations for this collection. 

        :param collection_id: The internal database ID
        :return techniques_list: returns the techniques as a list of technique short names
        """
        self.log.debug(f"Reading techniques associated with collection ID: {collection_id}")
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """SELECT t.short_name
                        FROM collection c 
                            JOIN collection_assoc_technique cat ON c.id = cat.collection_id 
                            JOIN technique t ON cat.technique_id = t.id
                        WHERE collection_id = %s
                    """
            data = (collection_id, )
            self.log.debug(f"Executing stmt {stmt} with data {data}")
            cursor.execute(stmt, data)
            techniques_list = []
            for row in cursor.fetchall():
                short_name = row[0]
                techniques_list.append(short_name)

            return techniques_list

        except Exception as error:
            self.log.error("Failed in _read_technique_associations() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                if mysql_cnx:
                    self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def create_project_associations(self, collection_id, projects_list):
        """
        Create project associations for this collection. 

        :param collection_id: The internal database ID
        :param projects_list: The list of source project names
        :return: returns nothing
        """
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            self._create_project_associations(collection_id, projects_list, cursor) 
            mysql_cnx.commit()
        except Exception as error:
            mysql_cnx.rollback()
            raise error                 
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def _create_project_associations(self, collection_id, projects_list, cursor):
        """
        Create project associations for this collection. This method is called in the context of an existing 
        transaction, so it does not commit the changes. The calling method should ensure changes are committed.

        :param collection_id: The internal database ID
        :param projects_list: The list of project short names
        :param cursor: An cursor object on the connection
        :return: returns nothing
        """

        try:
            # Insert the projects associated with this collection
            self.log.debug(projects_list)
            for short_name in projects_list:
                self.log.debug(f"Processing project {short_name}")
                p = project.Project(self.log)
                proj = p.get_grant_or_project_by_name(short_name)
                if not proj:
                    self.log.error(f"Cannot find project with name: {short_name}")
                    raise Exception(f"Cannot find project with name: {short_name}")

                project_id = proj.id

                stmt = """INSERT INTO collection_assoc_project (collection_id, project_id)
                                VALUES (%s, %s)
                """
                data = (collection_id, project_id)
                self.log.debug(f"Executing stmt {stmt} with data {data}")
                cursor.execute(stmt, data)

        except Exception as error:
            self.log.error("Failed in _create_project_associations() {}".format(error), exc_info=sys.exc_info())
            raise error

    def _read_project_associations(self, collection_id):
        """
        Read project associations for this collection. 

        :param collection_id: The internal database ID
        :return projects_list: returns the projects as a list of project short names
        """
        self.log.debug(f"Reading projects associated with collection ID: {collection_id}")
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """SELECT p.short_name
                        FROM collection c 
                            JOIN collection_assoc_project cat ON c.id = cat.collection_id 
                            JOIN project p ON cat.project_id = p.id
                        WHERE collection_id = %s
                    """
            data = (collection_id, )
            self.log.debug(f"Executing stmt {stmt} with data {data}")
            cursor.execute(stmt, data)
            projects_list = []
            for row in cursor.fetchall():
                short_name = row[0]
                projects_list.append(short_name)

            return projects_list

        except Exception as error:
            self.log.error("Failed in _read_project_associations() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                if mysql_cnx:
                    self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def add_publication(self, collection_id, publication):
        """
        Add publication for this collection. 

        :param collection_id: The internal database ID
        :param publication: The publication information
        :return: returns nothing
        """
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            self._add_publication(collection_id, publication, mysql_cnx, cursor) 
            mysql_cnx.commit()
        except Exception as error:
            mysql_cnx.rollback()
            raise error                 
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def _add_publication(self, collection_id, publication, cursor):
        """
        Create publication associations for this collection. This method is called in the context of an existing 
        transaction, so it does not commit the changes. The calling method should ensure changes are committed.

        :param collection_id: The internal database ID
        :param publication: The publication
        :param cursor: An cursor object on the connection
        :return: returns nothing
        """
        # Verify that all required fields are specified, else raise exception
        if ("title" not in publication  or "pub_doi" not in publication or "pub_year" not in publication or "journal" not in publication) :
            raise Exception("Missing one of the required parameters: title, pub_doi, pub_year, journal, collection_id") 

        try:
            # Insert the publication associated with this collection
            self.log.debug(publication)

            stmt = """INSERT INTO publication (title, pub_doi, pub_year, journal, collection_id)
                                VALUES (%s, %s, %s, %s, %s)
                """
            data = (publication['title'], publication['pub_doi'], publication['pub_year'], publication['journal'], collection_id)

            # add to statement with optional fields
            if 'pubmed_id' in publication:
                ins_stmt = ins_stmt + ", pubmed_id"
                val_stmt = val_stmt + ", %s"
                data = (*data, publication['pubmed_id'])

            if 'vol' in publication:
                ins_stmt = ins_stmt + ", vol"
                val_stmt = val_stmt + ", %s"
                data = (*data, publication['vol'])

            if 'page' in publication:
                ins_stmt = ins_stmt + ", page"
                val_stmt = val_stmt + ", %s"
                data = (*data, publication['page'])

            if 'pub_status' in publication:
                ins_stmt = ins_stmt + ", pub_status"
                val_stmt = val_stmt + ", %s"
                data = (*data, publication['pub_status'])


            self.log.debug(f"Executing stmt {stmt} with data {data}")
            cursor.execute(stmt, data)

        except Exception as error:
            self.log.error("Failed in _add_publication() {}".format(error), exc_info=sys.exc_info())
            raise error

    def _read_publication(self, collection_id):
        """
        Read publication associations for this collection. 

        :param collection_id: The internal database ID
        :return publication: returns the publication as a dictionary.
        """
        self.log.debug(f"Reading publication associated with collection ID: {collection_id}")
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """SELECT title, pub_doi, pub_year, pubmed_id, journal, vol, page, pub_status
                        FROM publication 
                        WHERE collection_id = %s
                    """
            data = (collection_id, )
            self.log.debug(f"Executing stmt {stmt} with data {data}")
            cursor.execute(stmt, data)
            publication = {}
            for row in cursor.fetchall():
                publication['title'] = row[0]
                publication['pub_doi'] = row[1]
                publication['pub_year'] = row[2]
                publication['pubmed_id'] = row[3]
                publication['journal'] = row[4]
                publication['vol'] = row[5]
                publication['page'] = row[6]
                publication['pub_status'] = row[7]

            return publication

        except Exception as error:
            self.log.error("Failed in _read_publication() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                if mysql_cnx:
                    self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error


    def add_entity_url(self, collection_id, entity_url):
        """
        Add entity urls for this collection.

        :param collection_id: The internal database ID
        :param entity_url: The entity url information passed as a dictionary
        :return: returns nothing
        """
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            self._add_entity_url(collection_id, entity_url, mysql_cnx, cursor)
            mysql_cnx.commit()
        except Exception as error:
            mysql_cnx.rollback()
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    def _add_entity_url(self, collection_id, entity_url, cursor):
        """
        Create entity_url associations for this collection. This method is called in the context of an existing
        transaction, so it does not commit the changes. The calling method should ensure changes are committed.

        :param collection_id: The internal database ID
        :param entity_url: The entity_url passed as a dictionary
        :param cursor: An cursor object on the connection
        :return: returns nothing
        """
        # Verify that all required fields are specified, else raise exception
        if ("entity_type" not in entity_url  or "urls_type" not in entity_url or "collection_id" not in entity_url) :
            raise Exception("Missing one of the required parameters: enity_type, entity_url, collection_id")

        try:
            # Insert the entity_url associated with this collection
            self.log.debug(entity_url)

            stmt = """INSERT INTO entity_has_urls (entity_type,urls_type,collection_id)
                                VALUES (%s, %s, %s)
                """
            data = (entity_url['entity_type'], entity_url['urls_type'], collection_id)

            # add to statement with optional fields
            if 'file_count' in entity_url:
                ins_stmt = ins_stmt + ", file_count"
                val_stmt = val_stmt + ", %s"
                data = (*data, entity_url['file_count'])

            if 'size' in entity_url:
                ins_stmt = ins_stmt + ", size"
                val_stmt = val_stmt + ", %s"
                data = (*data, entity_url['size'])

            if 'release' in entity_url:
                ins_stmt = ins_stmt + ", release"
                val_stmt = val_stmt + ", %s"
                data = (*data, entity_url['release'])

            if 'url_readme' in entity_url:
                ins_stmt = ins_stmt + ", url_readme"
                val_stmt = val_stmt + ", %s"
                data = (*data, entity_url['url_readme'])

            self.log.debug(f"Executing stmt {stmt} with data {data}")
            cursor.execute(stmt, data)

        except Exception as error:
            self.log.error("Failed in _add_entity_url() {}".format(error), exc_info=sys.exc_info())
            raise error

    def _read_entity_urls(self, collection_id):
        """
        Read entity_url associations for this collection.

        :param collection_id: The internal database ID
        :return entity_urls: returns the entity_urls as a dictionary.
        """
        self.log.debug(f"Reading entity_urls associated with collection ID: {collection_id}")
        mysql_cnx = None
        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()
            stmt = """SELECT entity_type, urls_type, file_count, size, `release`, url_readme
                FROM entity_has_urls
                WHERE collection_id = %s
                    """
            data = (collection_id, )
            self.log.debug(f"Executing stmt {stmt} with data {data}")
            cursor.execute(stmt, data)
            
            entity_url_list = []
            for row in cursor.fetchall():
                entity_url = {}
                entity_url['entity_type'] = row[0]
                entity_url['urls_type'] = row[1]
                entity_url['file_count'] = row[2]
                entity_url['size'] = row[3]
                entity_url['release'] = row[4]
                entity_url['url_readme'] = row[5]
                entity_url_list.append(entity_url)
            return entity_url_list
            

        except Exception as error:
            self.log.error("Failed in _read_entity_urls() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                if mysql_cnx:
                    self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error
    

    def delete(self):
        """
        Used to delete a collection and associated records in related tables.

        Parameters: None
        Returns: None
        """
        
        mysql_cnx = None

        if not self.id:
            raise Exception("Instance does not have required attribute set: 'id'")

        try:
            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()

            TABLE = "collection"
            ASSOCIATIONS = {
                "anatomies" : {
                    "table" : "collection_assoc_anatomy",
                    "id_col" : "collection_id"
                },
                "files" : {
                    "table" : "file_in_collection",
                    "id_col" : "collection_id"
                },
                "assays" : {
                    "table" : "collection_assoc_assay",
                    "id_col" : "collection_id"
                },
                "modalities" : {
                    "table" : "collection_assoc_modality",
                    "id_col" : "collection_id"
                },
                "projects" : {
                    "table" : "collection_assoc_project",
                    "id_col" : "collection_id"
                },
                "taxonomies" : {
                    "table" : "collection_assoc_species",
                    "id_col" : "collection_id"
                },
                "techniques" : {
                    "table" : "collection_assoc_technique",
                    "id_col" : "collection_id"
                },
                "attributes" : {
                    "table" : "collection_attributes",
                    "id_col" : "collection_id"
                },
                "collection_defined_by" : {
                    "table" : "collection_defined_by_file",
                    "id_col" : "collection_id"
                },
                "child_collections" : {
                    "table" : "collection_has_collection",
                    "id_col" : "parent_collection_id"
                },
                "parent_collections" : {
                    "table" : "collection_has_collection",
                    "id_col" : "child_collection_id"
                },                
                "contributors" : {
                    "table" : "collection_has_contributor",
                    "id_col" : "collection_id"
                },
                "subjects" : {
                    "table" : "subject_assoc_collection",
                    "id_col" : "collection_id"
                },
                "samples" : {
                    "table" : "sample_assoc_collection",
                    "id_col" : "collection_id"
                },
                "data_use_limitations" : {
                    "table" : "collection_has_data_use_condition",
                    "id_col" : "collection_id"
                },
                "keywords" : {
                    "table" : "collection_has_keywords",
                    "id_col" : "collection_id"
                }
            }
            
            for assoc_name in ASSOCIATIONS:
                assoc_details = ASSOCIATIONS[assoc_name]
                stmt = f"DELETE FROM {assoc_details['table']} WHERE {assoc_details['id_col']} = %({assoc_details['id_col']})s"
                self.log.debug(f"Executing stmt: {stmt} with value = {self.id}")
                cursor.execute(stmt, {f"{assoc_details['id_col']}" : self.id})
            
            stmt = f"DELETE FROM {TABLE} WHERE id = %(id)s"
            self.log.debug(f"Executing stmt: {stmt} with id: {self.id}")
            cursor.execute(stmt, {"id" : self.id})
            mysql_cnx.commit()

        except Exception as error:
            mysql_cnx.rollback()
            self.log.error("Failed in delete() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

