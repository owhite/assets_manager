global _transaction_state
_transaction_state = False

global _singleton_connector
_singleton_connector = None

def begin_txn():
    global _transaction_state
    _transaction_state = True

def commit_txn():
    global _singleton_connector
    if _singleton_connector:
        _singleton_connector.commit_transaction()
    else:
        print("Can't commit transaction. Not yet connected to DB")

def rollback_txn():
    global _singleton_connector
    if _singleton_connector:
        _singleton_connector.rollback_transaction()
    else:
        print("Can't rollback transaction. Not yet connected to DB")
    

def set_connector(cnx):
    global _singleton_connector
    _singleton_connector = cnx

def get_connector():
    return _singleton_connector

def is_transaction_state():
    return _transaction_state

def set_transaction_state(b):
    global _transaction_state
    _transaction_state = b

    