import configparser
import os.path


def read_config():
    configs = configparser.ConfigParser()
    configs.read(os.path.join(os.path.dirname(__file__), '../properties.ini'))
    return configs
