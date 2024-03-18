import sys
import itertools
from datetime import date, datetime
from typing import Any
import db_utils

class Base(object):

    def __init__(self, log, ctype, params={}):
        if not log:
            log = db_utils.StubLog()  
        self.log = log
        self.db = db_utils.Db(log)

        # Captures the child class type
        self.ctype = ctype

        for field in self.ATTRS:
            if field in params:
                setattr(self, field, params[field])
            else:
                setattr(self, field, None)

    def __str__(self):
        d = vars(self).copy()
        d.pop("log")
        d.pop("db")
        d.pop("ctype")
        return str(d)

    def __repr__(self):
        d = vars(self).copy()
        d.pop("log")
        d.pop("db")
        return str(d)

    def __getattribute__(self, name: str) -> Any:
        """
        There are essentially two types of proprties that a child class can have on it:
            * Those with raw values in a list
            * Regular simple strings, etc
        
        If this is the type that contains a human readable list of values, sometimes the calling
        application would want that human readable list (returned by default) and sometimes the
        underlying raw database values are needed.

        For example, internally, we're storing attributes like sample.anatomies as a dict:
            {
                'human_readable': ['AUD'], 
                'raw': [{'sample_assoc_anatomy.sample_id': 1, 'sample_assoc_anatomy.anatomy_id': 252}]
            }
        
        By default, if a client app requests sample.anatomies, they will only get the human readable list. If however, they want the
        full dict, they can request it using sample.anatomies_raw. This will return the full dict (both human readable and raw values)
        """
        if name.endswith("_raw"):
            raw_value = object.__getattribute__(self, name[0:-4]) # strip off _raw to get to the actual attribute
            return raw_value["raw"]

        if name.endswith("_all"):
            raw_value = object.__getattribute__(self, name[0:-4]) # strip off _raw to get to the actual attribute
            return raw_value

        # If _raw was not requested, check if this attribute is a dict with the key 'human_readable'
        # If so, return only the human readable list
        if isinstance(object.__getattribute__(self, name), dict):
            if 'human_readable' in object.__getattribute__(self, name):
               return object.__getattribute__(self, name)['human_readable']
        
        # Return the unadulterated attribute
        return object.__getattribute__(self, name)


    def _get_records_with_associations(self, params, assoc):
    
        # Get the FIELDS and TABLE from the child who is invoking this function
        child = self.ctype(self.log)
        FIELDS = child.FIELDS
        TABLE = child.TABLE

        mysql_cnx = None
        try:
            where_stmt = ""
            if not isinstance(params, dict):
                raise ValueError(f"params must be dict. Received {params} with type {type(params)}")
            fields_plus_id_col = list(FIELDS.keys()) + ["id"]
            if not all(field in fields_plus_id_col for field in list(params.keys())):
                raise ValueError(f"Expected only the following parameters {fields_plus_id_col}, but recieved params {list(params.keys())}")
            if not isinstance(assoc, list):
                raise ValueError(f"Associations must be provided as a list")
            if not all(assoc_key in child.ASSOCIATIONS.keys() for assoc_key in assoc):
                raise ValueError(f"Expected only the following associations {list(child.ASSOCIATIONS.keys())}, but recieved params {assoc}")
            
            # Build the where clause
            param_names = list(params.keys())
            for field in param_names:
                if where_stmt:
                    where_stmt += " and "
                else:
                    where_stmt = " WHERE "
                if params[field] is None:
                    where_stmt += f"{TABLE}.{field} is %({field})s"
                elif isinstance(params[field], list):
                    where_stmt += f"{TABLE}.{field} in ("
                    values_list = params[field]
                    for index, value in enumerate(values_list):
                        if index > 0:
                            where_stmt += ", "
                        where_stmt += f"%({field}_{str(index)})s"
                        params[field + "_" + str(index)] = value
                    where_stmt += ")"
                    params.pop(field)
                else:
                    where_stmt += f"{TABLE}.{field} = %({field})s"


            # Build and run the queries
            # A couple ways to do the association queries:
            # 1) Join in all of the tables together, thereby multiplying the number of results by the number of results from each association table
            #   Eg. 2 subjects, each with 10 subject_attributes and 4 subject_taxonomy = 80 records returned with a lot of de-duping that needs to happen
            #       Probably less SQL time, more Python time (filtering)
            # 2) Run a separate query for each of the association tables:
            #   Eg. 20 subject_attributes returned; then 4 subject_taxonomy records returned. 2 queries
            #       More SQL time, less Python time (no filtering needed)
            # Going with option 2 because it seems simpler to implement and possibly faster

            results = {}

            table_cols = [TABLE + "." + field for field in FIELDS.keys()]
            table_cols.insert(0, TABLE + ".id")
            aliased_table_cols = [col + " AS `" + col + "`" for col in table_cols]
            # aliased_table_cols = [TABLE + "." + field + " AS `" + TABLE + "." + field + "`" for field in FIELDS.keys()]
            # aliased_table_cols.insert(0, TABLE + ".id AS `" + TABLE + ".id`") # in addition to retrieving the named fields, we'll retrieve the internal table id
            # table_cols = [TABLE + "." + field for field in FIELDS.keys()]
            # table_cols.insert(0, TABLE + ".id")

            for assoc_name in assoc:
                self.log.debug(f"Now retrieving {assoc_name}")
                assoc_details = child.ASSOCIATIONS[assoc_name]

                # TODO: Should change all ASSOICATIONS to have a retrieve list of columns and a load_associations list of columns
                # For now, remove the id_col from assoc_cols to make life easy and have the return be a list instead of it being a dict (below)
                assoc_cols = []
                ref_cols = []
                for assoc_field in assoc_details['cols']:
                    if assoc_field == assoc_details['id_col']:
                        continue
                    assoc_cols.append(assoc_details['table'] + "." + assoc_field)
                if "ref_join" in assoc_details and "ref_cols" in assoc_details["ref_join"]:
                    for assoc_field in assoc_details['ref_join']['ref_cols']:
                        if assoc_field == assoc_details['id_col']:
                            continue
                        ref_cols.append(assoc_details['ref_join']['ref_table'] + "." + assoc_field)

                # assoc_cols = [assoc_details['table'] + "." + assoc_field for assoc_field in assoc_details['cols']]
                aliased_assoc_cols = [col + " AS `" + col + "`" for col in assoc_cols]
                assoc_id_col = f"{TABLE}.id"
                if 'assoc_id_col' in assoc_details:
                    assoc_id_col = assoc_details['assoc_id_col']
                
                # Originally, this was just a join, but now that I'm pulling back library which might have a null technique id, for example,
                # that results in no library coming back with a regular join. So I thought to do a left join instead, but that's caused me to
                # get back a lot of None results which messes up the compare_to() function by comparing things like an empty to list 
                # to a list containting a dict with no values:
                # Eg. {'attributes': {'arg': 'Empty list', 'db': "[{'value': None, 'unit': None, 'attributes_id': None, 'source_value': None}]"}
                # So to get around both of the shortcomings, will do a left join and then strip out dictionaries where all values are None
                join_stmt = f" left join {assoc_details['table']} on {assoc_details['table']}.{assoc_details['id_col']} = {assoc_id_col} "
                
                ref_details = None
                if "ref_join" in assoc_details:
                    ref_details = assoc_details["ref_join"]
                    aliased_ref_cols = [f"{ref_details['readable_field']} AS `{ref_details['ref_table']}.{ref_details['readable_field']}`"]
                    join_stmt += f" join {ref_details['ref_table']} on {ref_details['ref_table']}.id = {assoc_details['table']}.{ref_details['ref_field']} "
                    # select all table cols + all joined table cols
                    if "ref_cols" in ref_details:
                        aliased_ref_cols = [f"{ref_details['ref_table']}.{ref_col} AS `{ref_details['ref_table']}.{ref_col}`" for ref_col in ref_details['ref_cols']]
                    select = f""" SELECT {", ".join(aliased_table_cols + aliased_assoc_cols + aliased_ref_cols)} FROM {TABLE} """
                else:
                    select = f""" SELECT {", ".join(aliased_table_cols + aliased_assoc_cols)} FROM {TABLE} """

                # Normally, we don't want to concatenate anything to an sql string due to risk of SQL injections, however
                # all that's happening here is that we're building out a prepared statement with parameterized placeholders
                # in the 'where' clause. Additionally, these params and fields have been demonstrated to be valid (above).
                stmt = select + join_stmt + where_stmt

                mysql_cnx = self.db.get_db_connection()
                cursor = mysql_cnx.cursor(dictionary=True)

                self.log.debug(f"Executing stmt: {stmt} with params {params}")
                cursor.execute(stmt, params)
                for row in cursor.fetchall():
                    # field_names = [i[0] for i in cursor.description]
                    if row[f'{TABLE}.id'] in results:
                        # We've already pulled in this entity (fist-class table result)
                        instance = results[row[f'{TABLE}.id']]
                    else:
                        # Make the first class entity instance
                        entity_results = {k[len(TABLE)+1:]: row[k] for k in table_cols}
                        self.log.debug(f"Instantiating new entity with {entity_results}")
                        child_instance = self.ctype(self.log, entity_results)
                        results[row[f'{TABLE}.id']] = child_instance
                        instance = child_instance
                    
                    # append the results from this association query
                    # In order to accommodate both the human readable values as well as internal database values,
                    # we'll return a dictionary which has two keys if we were supplied with the reference table details
                    # (meaning that the reference table was also joined into the query (above))
                    # 'human_readable' and 'raw'
                    
                    if "ref_join" in assoc_details and "ref_cols" in assoc_details["ref_join"]:
                        raw_results = {col[len(assoc_details['ref_join']['ref_table'])+1:]:row[col] for col in ref_cols}
                    else:
                        raw_results = {col[len(assoc_details['table'])+1:]:row[col] for col in assoc_cols}
                    
                    # since we are left-joining, if we got back only None results, continue
                    if all(value is None for value in raw_results.values()):
                        continue

                    if ref_details:
                        # initialize this assocation to a dict with two keys (if it's not truthy)
                        if not getattr(instance, assoc_name):
                            setattr(instance, assoc_name, {'human_readable' : [], 'raw' : []}) 
                        human_results = row[f"{ref_details['ref_table']}.{ref_details['readable_field']}"]
                        current_assoc_dict = getattr(instance, assoc_name + "_all")
                        current_assoc_dict['human_readable'].append(human_results)
                        current_assoc_dict['raw'].append(raw_results)
                    else:
                        # If there are no reference details, we won't use the human_readable / raw keys at all in this attribute
                        # Instead, we'll just put the raw values in as a list of dicts (or just dict if one-to-one is specified)
                        
                        # if this is a one-to-one relationship, no need to use a list
                        if "one_to_one" in assoc_details and assoc_details["one_to_one"]:
                            setattr(instance, assoc_name, raw_results)
                        else:
                            # initialize this assocation to an empty list (if it's not already truthy)
                            if not getattr(instance, assoc_name):
                                setattr(instance, assoc_name, [])
                            current_assoc_list = getattr(instance, assoc_name)

                            # Instead of returning a list of dicts that have just one key, return a list of values.
                            if len(raw_results) == 1:
                                single_value = list(raw_results.values())[0]
                                current_assoc_list.append(single_value)
                            else:
                                current_assoc_list.append(raw_results)
                            
                            setattr(instance, assoc_name, current_assoc_list)

            self.log.debug("Returning " + str(len(results)) + " results")
            
            return list(results.values())

        except Exception as error:
            self.log.error("Failed in _get_records_with_associations() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error
        

    def _get_records_without_associations(self, params):

        # Get the FIELDS and TABLE from the child who is invoking this function
        child = self.ctype(self.log)
        FIELDS = child.FIELDS
        TABLE = child.TABLE

        mysql_cnx = None
        try:
            where_stmt = ""
            if not isinstance(params, dict):
                raise ValueError(f"params must be dict. Received {params} with type {type(params)}")
            fields_plus_id_col = list(FIELDS.keys()) + ["id"]
            if not all(field in fields_plus_id_col for field in list(params.keys())):
                raise ValueError(f"Expected only the following parameters {fields_plus_id_col}, but recieved params {list(params.keys())}")

            all_fields = [TABLE + "." + field for field in FIELDS.keys()]
            all_fields.insert(0, TABLE + ".id")

            select = f""" SELECT {", ".join(all_fields)}
                        FROM {TABLE}
                    """
            # Build the where clause
            param_names = list(params.keys())
            for field in param_names:
                if where_stmt:
                    where_stmt += " and "
                else:
                    where_stmt = " WHERE "
                if params[field] is None:
                    where_stmt += f"{field} is %({field})s"
                elif isinstance(params[field], list):
                    where_stmt += f"{field} in ("
                    values_list = params[field]
                    for index, value in enumerate(values_list):
                        if index > 0:
                            where_stmt += ", "
                        where_stmt += f"%({field}_{str(index)})s"
                        params[field + "_" + str(index)] = value
                    where_stmt += ")"
                    params.pop(field)
                else:
                    where_stmt += f"{field} = %({field})s"          


            # Normally, we don't want to concatenate anything to an sql string due to risk of SQL injections, however
            # all that's happening here is that we're building out a prepared statement with parameterized placeholders
            # in the 'where' clause. Additionally, these params and fields have been demonstrated to be valid (above).
            stmt = select + where_stmt

            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor(dictionary=True)

            self.log.debug(f"Executing stmt: {stmt} with params {params}")
            cursor.execute(stmt, params)

            results = []
            for row in cursor.fetchall():
                child_instance = self.ctype(self.log, row)
                results.append(child_instance)

            self.log.debug("Returning " + str(len(results)) + " results")
            return results

        except Exception as error:
            self.log.error("Failed in _get_records_without_associations() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error

    

    def _get_records(self, params={}, assoc=[]):
        if assoc:
            return self._get_records_with_associations(params, assoc)
        return self._get_records_without_associations(params)


    def _get_record(self, params:dict, assoc=[]):
        records = self._get_records(params, assoc)
        if records and len(records) == 1:
            return records[0]
        elif len(records) > 1:
            # for r in records:
            #     print(str(r))
            raise ValueError("Database call resulted in more than 1 record being returned. Use get_records() when more than one result is expected.")
        return None

    def _validate_add_keys(self, params):
        child = self.ctype(self.log)
        for field in child.REQUIRED_KEYS_FOR_ADD:
            if isinstance(field, str):
                if field not in params:
                    self.log.warning(f"{field} not in params")
                    self.log.debug("RETURNING FALSE!")
                    return False
            else:
                if not any(f in params for f in field):
                    self.log.debug("RETURNING FALSE")
                    return False
        return True
                    
            

    def _build_insert_stmt(self, params={}):
        child = self.ctype(self.log)
        FIELDS = child.FIELDS
        TABLE = child.TABLE

        REQUIRED_FIELDS = [field for field in FIELDS if FIELDS[field]]
        OPTIONAL_FIELDS = [field for field in FIELDS if not FIELDS[field]]

        # Verify that all required fields are specified, else raise exception
        if not all(req_field in params for req_field in REQUIRED_FIELDS):
            raise Exception(f"Missing required param. Required: {REQUIRED_FIELDS}. Received: {params}")
        
        # Build the start of the query with required fields
        ins_stmt = f"INSERT INTO {TABLE} ({','.join(REQUIRED_FIELDS)}"
        val_stmt = f"VALUES({', '.join(['%s'] * len(REQUIRED_FIELDS))}"

        # data = (sample['smp_id'],)
        data = tuple([params[field] for field in REQUIRED_FIELDS])

        # add to statement with optional fields
        for field in OPTIONAL_FIELDS:
            if field in params:
                ins_stmt = ins_stmt + f", {field}"
                val_stmt = val_stmt + ", %s"
                data = (*data, params[field])

        # Close the SQL statement
        stmt = f"{ins_stmt} ) {val_stmt} )"
        return stmt, data
    
    def _delete_assoc(self, assoc, key_field, value, cursor):

        stmt = f"DELETE FROM {assoc['table']} WHERE {key_field} = %({key_field})s"
        self.log.debug(f"Deleting {assoc['table']} records where {key_field} = {value}")
        cursor.execute(stmt, {key_field : value})

    def _create_assoc(self, assoc, values, cursor):
        # """
        # Create an association entry in table assoc['table'] with columns assoc['cols']
        # This method is called in the context of an existing transaction, so it does 
        # not commit the changes. The calling method should ensure changes are committed.

        # :param assoc: a dict containing the table name and columns
        # :param values: a dict containing the column names and values to be inserted
        # :param cursor: An cursor object on the connection.
        # :return: None
        # """

        self.log.debug(f"Entered _create_assoc with args: assoc = {assoc} values = {values}")
        # Start with an empty list and only include columns in this association
        # table that we have values for. Otherwise, don't include those in the insert
        # stmt.
        cols = []
        for c in assoc['cols']:
            if c in values:
                cols.append(c)
        
        stmt = f"INSERT INTO {assoc['table']} ({','.join(['`' + c + '`' for c in cols])}) VALUES ({', '.join(['%(' + c + ')s' for c in cols])})"
        self.log.debug(f"Executing stmt {stmt} with values {values}")
        cursor.execute(stmt, values)

    def compare_to(self, params:dict):
        """
        This function compares this instance (self) vs a dictionary that represents 
        the instance's fields.
        
        What is considered "equal" is up to the calling app. For example, one app might
        use this function to set up some key/value pairs in a dictionary that represent 
        what that app would consider to be equal to the instance that was retrieved from
        the db.
        
        For a concrete example, a subject record in the DB might be considered to be equal
        to the arg dictionary (params) if all fields on this instance (db record) match 
        all fields in the dictionary AND certain associations on this instance have a similar 
        value in the dictionary; such as taxonomies and subject_attributes.

        Returns: None if self and the arg dict are "equal"; otherwise returns details about 
            the differences as a dict with the keys being the fields that differ and the values
            being a dict themselves that contain keys 'arg' and 'self' with values being the
            differences.
        """
        
        diffs = {}
        for field in params.keys():
            param_value = params[field]
            self_value = getattr(self, field, None)

            # if param value and self value are both falsey, consider them to be equal
            if not param_value and not self_value:
                continue

            if type(param_value) != type(self_value):
                if isinstance(self_value, datetime) and isinstance(param_value, str):
                    if self_value.strftime('%F %T.%f') != param_value:
                        diffs[field] = { 
                            "arg" : param_value, 
                            "db" : self_value
                        }
                elif (isinstance(self_value, str) != isinstance(param_value, str)): # if one is an str and the other isn't
                    if not isinstance(param_value, str): # if the param_value isn't a string
                        if str(param_value) != self_value:  # see if the two are still unequal after converting param to a string
                            diffs[field] = { 
                                "arg" : param_value, 
                                "db" : self_value
                            }
                    else: # if the db value isn't a string
                        if str(self_value) != param_value: # see if the two are still unequal after converting db value to a string
                            diffs[field] = { 
                                "arg" : param_value, 
                                "db" : self_value
                            }
                else:
                    diffs[field] = { 
                        "arg" : param_value, 
                        "db" : self_value
                    }
            elif isinstance(param_value, list):
                # If the param field contains a list, check if the lengths are the different
                # if len(param_value) != len(self_value):
                #     diffs[field] = { 
                #         "arg" : f"len is: {len(param_value)}", 
                #         "db" : f"len is: {len(self_value)}. Values: {param_value} -- {self_value}"
                #     }
                # TODO: Need to handle case where empty list is in arg or db
                # Check the type of value at index 0 in both arg and self
                if len(param_value) == 0 or len(self_value) == 0:
                    if len(param_value) == 0 and len(self_value) == 0:
                        diffs[field] = { 
                            "arg" : f"length was: {len(param_value)}",
                            "db" : f"length was: {len(self_value)}"
                        }
                    elif len(param_value) == 0:
                        diffs[field] = { 
                            "arg" : f"Empty list",
                            "db" : f"{self_value}"
                        }
                    else:
                        diffs[field] = { 
                            "arg" : f"{param_value}",
                            "db" : f"Empty list",
                        }
                elif type(param_value[0]) != type(self_value[0]):
                    diffs[field] = { 
                        "arg" : f"type is: {type(param_value[0])} first value is: {param_value[0]}",
                        "db" : f"type is: {type(self_value[0])} first value is: {self_value[0]}"
                    }
                else:
                    # if it's a list of dicts
                    if isinstance(param_value[0], dict):
                        # if the first dicts in the lists have different keys, that's enough to be a difference
                        if set(param_value[0].keys()) != set(self_value[0].keys()):
                            diffs[field] = { 
                                "arg" : f"keys are: {list(param_value[0].keys())}", 
                                "db" : f"keys are: {list(self_value[0].keys())}"
                            }
                        else:
                            # Otherwise, we're dealing with lists of dicts that need to be compared to each other
                            # (the one from the DB and the one that was passed in)

                            # Sort the two lists of dicts before comparison:
                            # Had originally sorted the list of dicts using a key, but ran into a case where that didn't work (no sort key)
                            # [{'attributes_id': 11, 'event_id': 43, 'value': 'ATV', 'unit': None, 'source_value': 'ATV; RTV; TRU'}
                            #  {'attributes_id': 11, 'event_id': 43, 'value': 'RTV', 'unit': None, 'source_value': 'ATV; RTV; TRU'}
                            #  {'attributes_id': 11, 'event_id': 43, 'value': 'TRU', 'unit': None, 'source_value': 'ATV; RTV; TRU'}]
                            # from operator import itemgetter
                            # key_field = f"{field}_id" # So, for subject's attributes, the key field will be attributes_id
                            # hash = hashlib.sha256(dict_string.encode()).hexdigest()
                            # return hash
                        
                            # list_1, list_2 = [sorted(l, key=itemgetter(key_field)) for l in (param_value, self_value)]
                            # list_1, list_2 = [sorted(l, key=lambda i: hashlib.sha256(str(i).encode()).hexdigest()) for l in (param_value, self_value)]
                            # pairs = zip(list_1, list_2)
                            # if (any(x != y for x, y in pairs)):
                            #     # pairs is now an exhausted iterator, so re-zip the lists to re-initialize it to build the error message
                            #     pairs = zip(list_1, list_2)
                            #     diffs.append({ field : f"List of dicts contained different values when sorted: {[(x, y) for x, y in pairs if x != y]}"})
                            diff = [i for i in param_value if i not in self_value]
                            if diff:
                                diffs[field] = {"values_missing_from_db" : diff}
                            diff = [i for i in self_value if i not in param_value]
                            if diff:
                                if field in diffs:
                                    diffs[field]["values_in_db_not_in_args"] = diff
                                else:
                                    diffs[field] = {"values_in_db_not_in_args" : diff}
                    else:
                        # We're not comparing lists of dicts; instead, we're comparing two simple lists
                        arg_set = set(param_value)
                        self_set = set(self_value)
                        if arg_set != self_set:
                            diffs[field] = {
                                "arg" : param_value,
                                "db" : self_value
                            }
            elif param_value != self_value:
                diffs[field] = {
                    "arg" : param_value,
                    "db" : self_value
                }
        if diffs:
            return diffs
        return None


    def delete(self):
        """
        Used to delete a record and associated records in related tables.

        Parameters: None
        Returns: None
        """
        
        mysql_cnx = None

        if not self.id:
            raise Exception("Instance does not have required attribute set: 'id'")
        try:


            mysql_cnx = self.db.get_db_connection()
            cursor = mysql_cnx.cursor()

            for assoc_name in self.ASSOCIATIONS:
                assoc_details = self.ASSOCIATIONS[assoc_name]
                stmt = f"DELETE FROM {assoc_details['table']} WHERE {assoc_details['id_col']} = %({assoc_details['id_col']})s"
                self.log.debug(f"Executing stmt: {stmt} with value = {self.id}")
                cursor.execute(stmt, {f"{assoc_details['id_col']}" : self.id})
            
            stmt = f"DELETE FROM {self.TABLE} WHERE id = %(id)s"
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


    def _breadth_first_search(self, find_ancestors:bool, flatten_results:bool):
        """
        Breadth-First-Search (BFS) going up or down from a record to all parents or children depending upon 'find_ancestors' param
        
        :param: find_ancestors (bool): If True, will search 'up' the tree via parents; if False will search 'down' the tree via children
        :param: flatten_results (bool): Will return all ancestors as a single list if True otherwise, default return
        
        :return: a list of lists where each index represents the level of ancestor/descendant and the nested list represents the ancestor IDs

        Say we are searching for library ancestors where:
          lib 5 has parents 6 and 7
            lib 6 has parent 8
            lib 7 has parent 9
              lib 9 has parent 10
         
        That would be returned as:
            [ [6, 7], [8, 9], [10] ]
        
        If the caller is interested in the highest level ancestors, the caller can simply grab those ids as list[-1].
        If the caller simply wants all ancestor IDs as a single list, the caller can simply extend all of the lists together.
        """
        mysql_cnx = None

        try:
            child = self.ctype(self.log)

            if not hasattr(child, "SELF_JOIN_TABLE"):
                raise NotImplementedError(f"The SELF_JOIN_TABLE attribute has not been set up for {self.ctype}")

            record_id = self.id

            visited = [record_id]
            queue = [(record_id, 0)]

            mysql_cnx = self.db.get_db_connection()
            
            self_join_table = child.SELF_JOIN_TABLE['table']
            if find_ancestors:
                next_node_field = child.SELF_JOIN_TABLE['parent_field']
                this_node_field = child.SELF_JOIN_TABLE['child_field']
            else:
                next_node_field = child.SELF_JOIN_TABLE['child_field']
                this_node_field = child.SELF_JOIN_TABLE['parent_field']

            cursor = mysql_cnx.cursor(dictionary=True)
            stmt = f"SELECT {next_node_field} from {self_join_table} where {this_node_field} = %(id)s"

            results = []
            while queue:
                next_id, level = queue.pop(0)
                params = { "id" : next_id }
                self.log.debug(f"Executing stmt: {stmt} with params {params}")
                cursor.execute(stmt, params)

                for row in cursor.fetchall():
                    next_node_id = row[next_node_field]
                    if next_node_id not in visited:
                        visited.append(next_node_id)
                        queue.append((next_node_id, level+1))
                        if len(results) < level+1:
                            results.append([])
                        results[level].append(next_node_id)

            if flatten_results:
                return list(itertools.chain.from_iterable(results))
            return results
                

        except Exception as error:
            self.log.error("Failed in _breadth_first_search() {}".format(error), exc_info=sys.exc_info())
            raise error
        finally:
            try:
                self.db.close_connection(mysql_cnx)
            except Exception as error:
                raise error


    def _get_ancestors(self, flatten_results=False):
        search_upwards = True
        return self._breadth_first_search(search_upwards, flatten_results)
    
    def _get_descendant(self, flatten_results=False):
        search_upwards = False
        return self._breadth_first_search(search_upwards, flatten_results)

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