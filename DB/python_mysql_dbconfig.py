from configparser import ConfigParser
from Util import FileUtil


def read_db_config(filename='c:/temp/jquant/config.ini', section='mysql'):
    """ Read database configuration file and return a dictionary object
    :param filename: name of the configuration file
    :param section: section of database configuration
    :return: a dictionary of database parameters
    """
    if not FileUtil.file_exist(filename):
        print("configuration file {} does not exit.".format(filter()))
        return

    # create parser and read ini configuration file
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to mysql
    db = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db[item[0]] = item[1]
# {'host': 'localhost', 'database': 'quant', 'user': 'jchang', 'password': 'shulin0803'}
    else:
        raise Exception('{0} not found in the {1} file'.format(section, filename))

    return db
