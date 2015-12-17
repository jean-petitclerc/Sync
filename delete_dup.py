import os
import sys
# import time
# import configparser
import sqlite3
import hashlib
import logging
from optparse import OptionParser
from sync import scan_dir
from sync import parse_configs
from sync import db_create_tables
from sync import db_remove_deleted

logging.basicConfig(level=logging.INFO, format=' %(asctime)s - %(levelname)s - %(message)s')

# Global variable
config = {}
conn = None  # DB handle


# Global constant
CONFIG_FILE = 'data' + os.sep + 'sync.cfg'


# parms
parm = {'delete': False, 'scan': False}


class File(object):
    def __init__(self, file_name, file_md5, file_mtime, file_size, dir_name, root_dir, rel_path):
        self.file_name = file_name
        self.file_md5 = file_md5
        self.file_mtime = file_mtime
        self.file_size = file_size
        self.dir_name = dir_name
        self.root_dir = root_dir
        self.rel_path = rel_path


def parse_options():
    # Use optparse to get parms
    usage = "usage: %prog [options] scope_dir keep_dir"
    parser = OptionParser(usage=usage)
    parser.add_option("-d", "--delete", dest="delete", action="store_true", default=False,
                      help="Delete the duplicate files that are not in the keep directory.")
    parser.add_option("-s", "--scan", dest="scan", action="store_true", default=False,
                      help="Scan the scope directory. The default is to use the existing DB.")
    (options, args) = parser.parse_args()
    return options, args  # options: copy, rejects; args: source_dir


def db_get_name(dir_name):
    """
    Build a hash value from the concatenation of source_dir
    :param dir_name: valeur à hasher pour faire le db_name
    """
    m = hashlib.md5()
    m.update(dir_name.encode('utf-8'))
    db_name = m.hexdigest() + ".db"
    return db_name


def list_dup(db_h, root_dir):

    sel_md5 = \
        '''
        select file_md5, count(*)
          from file
         where file_md5 in (select file_md5 from file where dir_name = ?)
         group by file_md5
        having count(*) > 1
        '''
    sel_dup = \
        '''
        select dir_name, file_name, file_size, file_mtime
          from file
         where file_md5 = ?
       '''

    try:
        cur_md5 = db_h.cursor()
        cur_dup = db_h.cursor()
        for row_md5 in cur_md5.execute(sel_md5, [root_dir]):
            file_md5 = row_md5[0]
            logging.info("Possible duplicates: %s" % file_md5)
            for row_dup in cur_dup.execute(sel_dup, [file_md5]):
                dir_name = row_dup[0]
                file_name = row_dup[1]
                file_size = row_dup[2]
                file_mtime = row_dup[3]
                logging.info("    Fichier......: %s" % dir_name + os.sep + file_name)
                logging.info("        Size.....: %i" % file_size)
                logging.info("        MTime....: %s" % file_mtime)
            logging.info(" ")
    except sqlite3.Error as x:
        logging.error("SQL Error: \n" + str(x))


def delete_dup(db_h, keep_dir):
    sel_md5 = \
        '''
        select file_md5, count(*)
          from file
         where file_md5 in (select file_md5 from file where dir_name = ?)
         group by file_md5
        having count(*) > 1
        '''
    sel_dup = \
        '''
        select dir_name, file_name
          from file
         where file_md5 = ?
           and dir_name <> ?
        '''
    del_dup = \
        '''
        delete from file
         where file_md5 = ?
           and dir_name <> ?
       '''

    try:
        cur_md5 = db_h.cursor()
        cur_dup = db_h.cursor()
        for row_md5 in cur_md5.execute(sel_md5, [keep_dir]):
            file_md5 = row_md5[0]
            logging.info("Possible duplicates: %s" % file_md5)
            for row_dup in cur_dup.execute(sel_dup, [file_md5, keep_dir]):
                dir_name = row_dup[0]
                file_name = row_dup[1]
                logging.info("    Ce fichier sera effacé: %s" % dir_name + os.sep + file_name)
                if os.path.isfile(keep_dir + os.sep + file_name):
                    os.remove(dir_name + os.sep + file_name)
                else:
                    logging.error("    Ce fichier ne peut pas être effacé. " +
                                  " Il n'existe pas dans le répertoire à conserver.")
            logging.info(" ")
            cur_dup.execute(del_dup, [file_md5, keep_dir])

    except sqlite3.Error as x:
        logging.error("SQL Error: \n" + str(x))


def main():
    global parm, config, conn
    logging.info('Début du programme ' + sys.argv[0])

    # Get parameters and validate them
    (options, args) = parse_options()
    parm["delete"] = options.delete
    parm["scan"] = options.scan
    if len(args) < 2:
        logging.error("Ce programme a besoin de deux arguments, le dossier source et le dossier à protéger.")
        return 8
    source_dir = args[0]
    protec_dir = args[1]
    if source_dir.endswith(os.sep):
        source_dir = source_dir[0:-1]
    if protec_dir.endswith(os.sep):
        protec_dir = protec_dir[0:-1]

    logging.info("Paramètres:")
    logging.info("    Dossier scope................................: %s" % source_dir)
    logging.info("    Dossier à protéger...........................: %s" % protec_dir)
    if not os.path.isdir(source_dir):
        logging.error("Le dossier source n'existe pas.")
        return 8
    if not os.path.isdir(protec_dir):
        logging.error("Le dossier à protéger n'existe pas.")
        return 8

    if parm["delete"]:
        logging.info("    Option de suppression........................: Oui")
    else:
        logging.info("    Option de suppression........................: Non")
    if parm["scan"]:
        logging.info("    Option de population de la BD................: Oui")
    else:
        logging.info("    Option de population de la BD................: Non")
    logging.info(" ")

    # Read config file
    config = parse_configs()
    logging.info("Configurations:")
    logging.info("    Fichier de configuration.....................: " + CONFIG_FILE)
    for ext in config['accept_list']:
        logging.info("    Accepted extension...........................: %s" % ext)
    for ext in config['reject_list']:
        logging.info("    Rejected extension...........................: %s" % ext)

    db_name = db_get_name(source_dir)
    db_path = source_dir + os.sep + db_name
    logging.info("    Database file................................: " + db_name)
    logging.info(" ")

    conn = sqlite3.connect(db_path)
    db_create_tables(conn)            # Create the DB objects
    if parm['scan']:
        scan_dir(conn, source_dir)    # Create inventory of the files in the source directory structure
    # Pu sûr que c'est une bonne idée...
    # if parm['delete']:
    #    delete_dup(conn, protec_dir)
    #    db_remove_deleted(conn)       # Remove deleted files from db
    # else:
    list_dup(conn, source_dir)

    conn.commit()
    conn.close()

    logging.info('Fin du programme ' + sys.argv[0])
    return 0


if __name__ == "__main__":
    sys.exit(main())
