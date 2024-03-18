import logging
import os
import pathlib
import logging
import config

logs = None
conf = config.Config.get_config()

def create_logger(logname):
    # create logger if does not exist
    l = logging.getLogger(logname)
    if not l.handlers:
        log_dir = conf['logging']['log_dir']
        if not os.path.exists(log_dir):
            pathlib.Path(log_dir).mkdir(parents=True, exist_ok=True)
        log_file = log_dir + os.sep + logname
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        log_level = conf['logging']['log_level'] # logging.DEBUG if debug else logging.INFO
        if log_level == "DEBUG":
            log_level = logging.DEBUG
        else:
            log_level = logging.INFO
        
        logHandler = logging.FileHandler(filename=log_file)
        logHandler.setFormatter(logging.Formatter(log_format))
        logHandler.setLevel(log_level)
        l.setLevel(log_level)
        l.addHandler(logHandler)

    global logs
    logs[logname] = l

    
def get_logger(logname):
    global logs
    if not logs:
        # initialize dict of loggers
        logs = {}
    if logname not in logs:
        # this logger not yet created, create it
        create_logger(logname)

    # return the logger
    return logs[logname]

    
