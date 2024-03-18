
import configparser
import os

class Config:

    __config = None

    @staticmethod
    def get_config():
        if not Config.__config:
            Config.__config = configparser.ConfigParser()
            Config.__config.read(os.path.join(os.path.dirname(__file__), '../conf', 'assets.ini'))
        return Config.__config

