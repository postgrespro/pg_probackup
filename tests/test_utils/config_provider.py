import configparser


def read_config(s3_config_file):
    config = configparser.ConfigParser()
    config.read_string('[fake-section]\n' + open(s3_config_file).read())

    return config['fake-section']
