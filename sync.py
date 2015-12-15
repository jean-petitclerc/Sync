import os
import sys
import time
import configparser
import sqlite3
import hashlib
import logging
import shutil
from optparse import OptionParser

logging.basicConfig(level=logging.INFO, format=' %(asctime)s - %(levelname)s - %(message)s')

# Global variable
config = {}
conn = None  # DB handle


# Global constant
CONFIG_FILE = 'data' + os.sep + 'sync.cfg'


# parms
parm = {'copy': False, 'rejects': False, 'backup': False}


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
    usage = "usage: %prog [options] source cible"
    parser = OptionParser(usage=usage)
    parser.add_option("-c", "--copy", dest="copy", action="store_true", default=False,
                      help="Copy the files to the target location, if needed.")
    (options, args) = parser.parse_args()
    return options, args  # options: copy, rejects; args: source_dir target_dir


def parse_configs():
    global config
    try:
        cfg_parser = configparser.ConfigParser()
        cfg_parser.read(CONFIG_FILE)
        config['db_file'] = cfg_parser['database']['DB_FILE']
    except Exception as x:
        logging.error("Could not read the configuration file: " + CONFIG_FILE)
        logging.error(x)
    return config


def db_get_name(source_dir, target_dir):
    """
    Build a hash value from the concatenation of source_dir and target_dir
    :param source_dir:
    :param target_dir:
    :return: a hash value digest suffixed with .db
    """
    m = hashlib.md5()
    temp = source_dir + target_dir
    m.update(temp.encode('utf-8'))
    db_name = m.hexdigest() + ".db"
    return db_name


def db_create_tables():
    """
    Create the DB objects.
    Each ddl statement is appended in an array.
    Each ddl in the array is sent for execution to sqlite3
    """

    global conn

    ddl = [
        '''
create table if not exists file (
  dir_name   text      not null,
  file_name  text      not null,
  file_md5   text      not null,
  file_mtime timestamp not null,
  file_size  int       not null,
  root_dir   text      not null,
  rel_path   text      not null
  )
;
        ''',
        '''
create unique index if not exists pk_file
  on file(dir_name, file_name)
;
        ''',
        '''
create index if not exists ix_file_01
  on file(file_md5)
;
        ''',
        '''
create index if not exists ix_file_02
  on file(root_dir, file_md5)
;
        ''',
        '''
create index if not exists ix_file_03
  on file(root_dir, rel_path, file_name)
;
        ''']

    try:
        c = conn.cursor()
        for stmt in ddl:
            c.execute(stmt)
    except sqlite3.Error as x:
        logging.error("SQL Error: \n" + str(x))


def db_store_file(file):
    global conn
    insert = \
        '''
        insert into file(dir_name, file_name, file_md5, file_mtime, file_size, root_dir, rel_path)
            values(?, ?, ?, ?, ?, ?, ?)
        '''
    update = \
        '''
        update file
           set file_md5   = ?,
               file_mtime = ?,
               file_size  = ?
         where dir_name   = ?
           and file_name  = ?
        '''

    select = \
        '''
        select file_md5, file_mtime, file_size
          from file
         where dir_name  = ?
           and file_name = ?
        '''

    try:
        cur = conn.cursor()
        cur.execute(select, [file.dir_name, file.file_name])
        row = cur.fetchone()
        if row is None:
            logging.debug("The file is NOT in the database.")
            ins = conn.cursor()
            ins.execute(insert, [file.dir_name, file.file_name, file.file_md5, file.file_mtime, file.file_size,
                                 file.root_dir, file.rel_path])
        else:
            logging.debug("The file is already in the database.")
            logging.debug("Comparing the md5/mtime/size.")
            if row[0] == file.file_md5 and row[1] == file.file_mtime and row[2] == file.file_size:
                logging.debug("Same file")
            else:
                logging.debug("Updating file md5, mtime and size.")
                upd = conn.cursor()
                upd.execute(update, [file.file_md5, file.file_mtime, file.file_size, file.dir_name, file.file_name])
    except sqlite3.Error as x:
        logging.error("SQL Error: \n" + str(x))


def db_remove_deleted():
    """
    Verifies that every file in the file table really exists on the filesystem.
    If not the entry is deleted from the table.
    :return: Nothing
    """

    global conn
    logging.info("Nettoyage de la BD pour les fichiers effacés.")
    count_found = 0
    count_notfound = 0

    select = \
        '''
        select dir_name, file_name
          from file
         order by dir_name, file_name
        '''

    delete = \
        '''
        delete from file
         where dir_name  = ?
           and file_name = ?
        '''

    try:
        cur = conn.cursor()
        for row in cur.execute(select, []):
            dir_name = row[0]
            file_name = row[1]
            file_path = dir_name + os.sep + file_name
            if os.path.isfile(file_path):
                count_found += 1
                logging.debug("Fichier existant.....: " + file_path)
            else:
                count_notfound += 1
                logging.debug("Fichier non-existant.: " + file_path)
                dlt = conn.cursor()
                dlt.execute(delete, [dir_name, file_name])
    except sqlite3.Error as x:
        logging.error("SQL Error: \n" + str(x))
    logging.info("Nettoyage terminé")
    logging.info("    Fichiers trouvés.............................: %i" % count_found)
    logging.info("    Fichiers manquants...........................: %i" % count_notfound)
    logging.info(" ")


def list_dup(root_dir):
    global conn

    sel_md5 = \
        '''
        select file_md5, count(*)
          from file
         where root_dir = ?
         group by file_md5
        having count(*) > 1
        '''
    sel_dup = \
        '''
        select dir_name, file_name, file_size, file_mtime
          from file
         where file_md5 = ?
           and root_dir = ?
       '''

    try:
        cur_md5 = conn.cursor()
        cur_dup = conn.cursor()
        for row_md5 in cur_md5.execute(sel_md5, [root_dir]):
            file_md5 = row_md5[0]
            logging.info("Possible duplicates: %s" % file_md5)
            for row_dup in cur_dup.execute(sel_dup, [file_md5, root_dir]):
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


def find_missing_files(source_dir, target_dir):
    global conn
    counts = {'copy': 0, 'compare': 0, 'kept': 0, 'newer': 0, 'older': 0}

    sel_src = \
        '''
        select dir_name, file_name, file_md5, file_mtime, rel_path
          from file
         where root_dir = ?
         order by dir_name, file_name
        '''
    sel_tgt = \
        '''
        select file_md5, file_mtime
          from file
         where root_dir  = ?
           and rel_path  = ?
           and file_name = ?
        '''

    try:
        cur_src = conn.cursor()
        cur_tgt = conn.cursor()
        for row_src in cur_src.execute(sel_src, [source_dir]):
            dir_name = row_src[0]
            file_name = row_src[1]
            file_md5_src = row_src[2]
            file_mtime_src = row_src[3]
            rel_path = row_src[4]
            cur_tgt.execute(sel_tgt, [target_dir, rel_path, file_name])
            row_tgt = cur_tgt.fetchone()
            if row_tgt is None:
                # Copy
                copy_file(dir_name, file_name, target_dir, rel_path)
                counts['copy'] += 1
            else:
                counts['compare'] += 1
                file_md5_tgt = row_tgt[0]
                file_mtime_tgt = row_tgt[1]
                # Compare
                if file_md5_src == file_md5_tgt:
                    logging.debug("Le fichier n'a pas à être copié.")
                    counts['kept'] += 1
                else:
                    if file_mtime_src > file_mtime_tgt:
                        logging.debug("Le fichier est plus récent et doit être copié.")
                        copy_file(dir_name, file_name, target_dir, rel_path)
                        counts['newer'] += 1
                    else:
                        logging.debug("Le fichier sur la cible est plus récent. Il ne sera pas écrasé.")
                        counts['older'] += 1
    except sqlite3.Error as x:
        logging.error("SQL Error: \n" + str(x))

    logging.info("Statistiques pour les copies:")
    logging.info("    Fichiers copiés..............................: %i" % counts['copy'])
    logging.info("    Comparaison requises.........................: %i" % counts['compare'])
    logging.info("        Copies évitées (même checksum)...........: %i" % counts['kept'])
    logging.info("        Fichiers remplacés par un plus récent....: %i" % counts['newer'])
    logging.info("        Fichiers cibles plus récents conservés...: %i" % counts['older'])
    logging.info(" ")
    return counts['copy'] + counts['newer']


def copy_file(dir_name, file_name, target_dir, rel_path):
    source_path = dir_name + os.sep + file_name
    if rel_path == '.':
        tgt_dir = target_dir
    else:
        tgt_dir = target_dir + os.sep + rel_path
    target_path = os.path.join(tgt_dir, file_name)
    logging.info("Copie de.........................................: %s" % source_path)
    logging.info("    vers.........................................: %s" % target_path)
    if parm["copy"]:
        os.makedirs(tgt_dir, exist_ok=True)
        shutil.copy2(source_path, target_path)
    else:
        logging.debug("Mode simulation: Fichier ne sera pas copié.")


def get_metadata(root_dir, dir_name, file_name):
    file_path = os.path.join(dir_name, file_name)
    (mode, ino, dev, nlink, uid, gid, file_size, atime, mtime, ctime) = os.stat(file_path)
    lastmod_date = time.localtime(mtime)
    file_mtime = time.strftime("%Y-%m-%d-%H.%M.%S", lastmod_date)

    # Open,close, read file and calculate MD5 on its contents
    with open(file_path, "rb") as file_to_check:
        # read contents of the file
        data = file_to_check.read()
        # pipe contents of the file through
        file_md5 = hashlib.md5(data).hexdigest()
    rel_path = os.path.relpath(dir_name, root_dir)
    logging.info("Fichiers: %s" % file_path)
    logging.debug("    Date modification (formatté).................: %s" % file_mtime)
    logging.debug("    Grosseur en bytes............................: %i" % file_size)
    logging.debug("    Checksum.....................................: %s" % file_md5)
    file = File(file_name, file_md5, file_mtime, file_size, dir_name, root_dir, rel_path)
    db_store_file(file)


def scan_dir(root_dir):
    logging.debug("Entrée dans scan_dir. Parm: " + root_dir)
    counts = {'epub': 0, 'gif': 0, 'jpg': 0, 'mp3': 0, 'pdf': 0, 'txt': 0, 'others': 0}
    logging.info("Inspection de " + root_dir)
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith("epub") or file.endswith("EPUB"):
                counts['epub'] += 1
                get_metadata(root_dir, root, file)
            elif file.endswith(".gif") or file.endswith(".GIF"):
                counts['gif'] += 1
                get_metadata(root_dir, root, file)
            elif (file.endswith(".jpg") or file.endswith(".jpeg") or
                  file.endswith(".JPG") or file.endswith(".JPEG")):
                counts['jpg'] += 1
                get_metadata(root_dir, root, file)
            elif file.endswith("mp3") or file.endswith("MP3"):
                counts['mp3'] += 1
                get_metadata(root_dir, root, file)
            elif file.endswith("pdf") or file.endswith("PDF"):
                counts['pdf'] += 1
                get_metadata(root_dir, root, file)
            elif file.endswith("txt") or file.endswith("TXT"):
                counts['pdf'] += 1
                get_metadata(root_dir, root, file)
            else:
                counts['others'] += 1
                logging.info("Type de fichiers exclus: " + os.path.join(root, file))
    logging.info(" ")
    logging.info("Statistiques pour " + root_dir)
    logging.info("    Fichiers epub trouvés........................: %i" % counts['epub'])
    logging.info("    Fichiers gif  trouvés........................: %i" % counts['gif'])
    logging.info("    Fichiers jpg  trouvés........................: %i" % counts['jpg'])
    logging.info("    Fichiers mp3  trouvés........................: %i" % counts['mp3'])
    logging.info("    Fichiers pdf  trouvés........................: %i" % counts['pdf'])
    logging.info("    Fichiers txt  trouvés........................: %i" % counts['txt'])
    logging.info("    Autres fichiers trouvés......................: %i" % counts['others'])
    logging.info(" ")
    logging.debug("Sortie de scan_dir.")


def main():
    global parm, conn, config
    logging.info('Début du programme ' + sys.argv[0])

    # Get parameters and validate them
    (options, args) = parse_options()
    parm["copy"] = options.copy
    if len(args) < 2:
        logging.error("Ce programme a besoin de deux arguments, le dossier source et le dossier cible.")
        return 8
    source_dir = args[0]
    target_dir = args[1]

    logging.info("Paramètres:")
    logging.info("    Dossier source...............................: %s" % source_dir)
    if not os.path.isdir(source_dir):
        logging.error("Le dossier source n'existe pas.")
        return 8

    logging.info("    Dossier cible................................: %s" % target_dir)
    if not os.path.isdir(target_dir):
        logging.warning("    Le dossier cible n'existe pas. Il sera créé.")
        os.makedirs(target_dir)

    if parm["copy"]:
        logging.info("    Option de copie..............................: On")
    else:
        logging.info("    Option de copie..............................: Off")
    logging.info(" ")

    # Read config file
    config = parse_configs()
    logging.info("Configurations:")
    if config['db_file'] is None:
        logging.error("The DB_FILE is missing from the [database] section in " + CONFIG_FILE)
        return 8
    logging.info("    Fichier de configuration.....................: " + CONFIG_FILE)
    # logging.info("    Database file................................: " + config['db_file'])

    db_name = db_get_name(source_dir, target_dir)
    db_path = source_dir + os.sep + db_name
    logging.info("    Database file................................: " + db_name)
    logging.info(" ")
    conn = sqlite3.connect(db_path)
    db_create_tables()        # Create the DB objects
    db_remove_deleted()       # Remove deleted files from db
    scan_dir(source_dir)      # Create inventory of the files in the source directory structure
    list_dup(source_dir)
    scan_dir(target_dir)      # Create inventory of the files in the target directory structure
    file_copied = find_missing_files(source_dir, target_dir)  # Identify files that need to be copied
    if file_copied > 0:
        scan_dir(target_dir)  # Update inventory of the files in the target directory structure
    list_dup(target_dir)
    conn.commit()
    conn.close()

    logging.info('Fin du programme ' + sys.argv[0])
    return 0


if __name__ == "__main__":
    sys.exit(main())