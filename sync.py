import os
import sys
import time
import configparser
import sqlite3
import hashlib
import logging
import shutil
import paramiko
import json
from optparse import OptionParser

# Global variable
config = {}
cred = {'host': '', 'port': 22, 'user': '', 'pswd': ''}
conn = None  # DB handle
ssh_client = None
ftp_client = None
os_sep_rmt = '/'

# Global constant
CONFIG_FILE = 'data' + os.sep + 'sync.cfg'
INDENT_SZ = 4
MSG_LGT = 60

# parms
parm = {'copy': False, 'remote': None, 'dup': 'N'}


class File(object):
    def __init__(self, file_name, file_md5, file_mtime, file_size, dir_name, root_dir, rel_path, local_rmt):
        self.file_name = file_name
        self.file_md5 = file_md5
        self.file_mtime = file_mtime
        self.file_size = file_size
        self.dir_name = dir_name
        self.root_dir = root_dir
        self.rel_path = rel_path
        self.local_rmt = local_rmt

    def __str__(self):
        return "File:\nDir: " + self.dir_name + "\nFile: " + self.file_name + "\nMD5: " + self.file_md5 + \
               "\nMTime: " + self.file_mtime + " Size: " + str(self.file_size) + " Local/Remote: " + self.local_rmt + \
               "\nRoot dir: " + self.root_dir + "\nRelative Path: " + self.rel_path


def parse_options():
    # Use optparse to get parms
    usage = "usage: %prog [options] source cible"
    parser = OptionParser(usage=usage)
    parser.add_option("-c", "--copie", dest="copy", action="store_true", default=False,
                      help="Copie les fichiers vers le répertoire cible.")
    parser.add_option("-d", "--doublons", dest="dup", action="store", default='N',
                      help="Liste les doublons sur la S(ource), C(ible) or T(ous).")
    parser.add_option("-r", "--remote", dest="remote", action="store", default=None,
                      help="Fichier pour les paramètres de connection pour les cibles distantes.")
    parser.add_option("-l", "--logging", dest="log", action="store", default='INFO',
                      help="Niveau de logging, DEBUG, INFO, ERROR, CRITICAL,...")
    (options, args) = parser.parse_args()
    if len(args) < 2:
        parser.error("Ce programme a besoin de deux arguments, le dossier source et le dossier cible.")
    if options.log.upper() not in ['INFO', 'DEBUG', 'WARNING', 'ERROR', 'CRITICAL']:
        parser.error("Option de logging invalide: %s" % options.log)

    return options, args  # options: copy, rejects; args: source_dir target_dir


def parse_configs():
    global config
    try:
        cfg_parser = configparser.ConfigParser()
        cfg_parser.read(CONFIG_FILE)
        # config['db_file'] = cfg_parser['database']['DB_FILE']
        config['accept_ext'] = cfg_parser['extensions']['ACCEPT_EXT']
        config['accept_list'] = config['accept_ext'].lower().split(',')
        config['reject_ext'] = cfg_parser['extensions']['REJECT_EXT']
        config['reject_list'] = config['reject_ext'].lower().split(',')
    except Exception as x:
        print_log('E', 0, msg="Could not read the configuration file", val=CONFIG_FILE)
        print_log('E', 0, val=str(x))
    return config


def parse_host_info(host_file):
    global cred
    try:
        cfg_parser = configparser.ConfigParser()
        cfg_parser.read(host_file)
        cred['host'] = cfg_parser['host']['SERVER']
        cred['user'] = cfg_parser['host']['USER']
        cred['pswd'] = cfg_parser['host']['PASS']
        port = cfg_parser['host']['PORT']
        if not port.isdigit():
            print_log('E', 0, msg="The port number must be numeric.")
            cred['valid'] = False
        else:
            cred['port'] = int(port)
            cred['valid'] = True
    except Exception as x:
        print_log('E', 0, msg="Could not read the configuration file: ", val=host_file)
        print_log('E', 0, val=str(x))
    return


def print_log(lvl, indent, msg=None, val=None, dotted=True):
    if msg is None:
        if val is None:
            message = " "
        else:
            message = val
    else:
        if val is None:
            message = indent * INDENT_SZ * " " + msg
        else:
            if dotted:
                message = indent * INDENT_SZ * " " + msg.ljust(MSG_LGT - indent * INDENT_SZ, '.') + ": %s" % val
            else:
                message = indent * INDENT_SZ * " " + msg + "%s" % val
    if lvl == 'C':
        logging.critical("" + message)
    elif lvl == 'E':
        logging.error("   " + message)
    elif lvl == 'W':
        logging.warning(" " + message)
    elif lvl == 'D':
        logging.debug("   " + message)
    else:  # Catches lvl = I and whatever else
        logging.info("    " + message)


def check_target_dir_rmt(target_dir):
    ssh_command = "ls '" + target_dir + "'"
    rc = ssh_command_with_rc(ssh_command)
    if rc == 0:
        return 0
    print_log('I', 2, msg="Le dossier cible n'existe pas. Il sera créé.")
    rc = ssh_command_with_rc("mkdir -m 750 -p '" + target_dir + "'")
    if rc > 0:
        print_log('I', 0, msg="The mkdir failed", val="RC=%i" % rc)
    return rc


def get_os_sep_rmt():
    command = "/home/jean/sync_rmt.py -o"
    stdin, stdout, stderr = ssh_client.exec_command(command)
    data = stdout.read().decode('utf-8')
    os_sep = data[0:1]
    return os_sep


def db_get_name(source_dir, target_dir, host):
    """
    Build a hash value from the concatenation of source_dir and target_dir
    :param source_dir:
    :param target_dir:
    :param host: If the target directory is on a remote host use the server name
    :return: a hash value digest suffixed with .db
    """
    m = hashlib.md5()
    if host is not None:
        temp = source_dir + host + target_dir
    else:
        temp = source_dir + target_dir
    m.update(temp.encode('utf-8'))
    db_name = m.hexdigest() + ".db"
    return db_name


def db_create_tables(db_h):
    """
    Create the DB objects.
    Each ddl statement is appended in an array.
    Each ddl in the array is sent for execution to sqlite3
    :param db_h: DB handle
    """

    ddl = [
        '''
create table if not exists file (
  dir_name   text      not null,
  file_name  text      not null,
  file_md5   text      not null,
  file_mtime timestamp not null,
  file_size  int       not null,
  root_dir   text      not null,
  rel_path   text      not null,
  local_rmt  text      not null
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
        c = db_h.cursor()
        for stmt in ddl:
            c.execute(stmt)
    except sqlite3.Error as x:
        print_log('E', 0, msg="SQL Error: ", val=str(x), dotted=False)


def db_store_file(db_h, file):
    insert = \
        '''
        insert into file(dir_name, file_name, file_md5, file_mtime, file_size, root_dir, rel_path, local_rmt)
            values(?, ?, ?, ?, ?, ?, ?, ?)
        '''
    update = \
        '''
        update file
           set file_md5   = ?,
               file_mtime = ?,
               file_size  = ?
         where dir_name   = ?
           and file_name  = ?
           and local_rmt  = ?
        '''

    select = \
        '''
        select file_md5, file_mtime, file_size
          from file
         where dir_name  = ?
           and file_name = ?
           and local_rmt = ?
        '''

    try:
        cur = db_h.cursor()
        cur.execute(select, [file.dir_name, file.file_name, file.local_rmt])
        row = cur.fetchone()
        ins = db_h.cursor()
        if row is None:
            print_log('D', 0, msg="The file is NOT in the database.")
            ins.execute(insert, [file.dir_name, file.file_name, file.file_md5, file.file_mtime, file.file_size,
                                 file.root_dir, file.rel_path, file.local_rmt])
        else:
            print_log('D', 0, msg="The file is already in the database.")
            print_log('D', 0, msg="Comparing the md5/mtime/size.")
            if row[0] == file.file_md5 and row[1] == file.file_mtime and row[2] == file.file_size:
                print_log('D', 0, msg="Same file")
            else:
                print_log('D', 0, msg="Updating file md5, mtime and size.")
                upd = db_h.cursor()
                upd.execute(update, [file.file_md5, file.file_mtime, file.file_size, file.dir_name, file.file_name,
                                     file.local_rmt])
    except sqlite3.Error as x:
        print_log('E', 0, msg="SQL Error: ", val=str(x), dotted=False)


def db_remove_deleted(db_h):
    """
    Verifies that every file in the file table really exists on the filesystem.
    If not the entry is deleted from the table.
    :param db_h: DB handle
    :return: Nothing
    """

    print_log('I', 0, msg="Nettoyage de la BD pour les fichiers effacés.")
    count_found = 0
    count_notfound = 0

    select = \
        '''
        select dir_name, file_name, local_rmt
          from file
         order by dir_name, file_name
        '''

    delete = \
        '''
        delete from file
         where dir_name  = ?
           and file_name = ?
           and local_rmt = ?
        '''

    try:
        cur = db_h.cursor()
        for row in cur.execute(select, []):
            dir_name = row[0]
            file_name = row[1]
            local_rmt = row[2]
            file_path = dir_name + os.sep + file_name
            if local_rmt == 'L':
                if os.path.isfile(file_path):
                    count_found += 1
                    print_log('D', 1, msg="Fichier existant.....: ", val=file_path, dotted=False)
                else:
                    count_notfound += 1
                    print_log('I', 1, msg="Fichier non-existant.: ", val=file_path, dotted=False)
                    dlt = db_h.cursor()
                    dlt.execute(delete, [dir_name, file_name, local_rmt])
            else:   # local_rmt == 'R'
                if os.sep in dir_name:
                    dir_name = dir_name.replace(os.sep, os_sep_rmt)
                file_path = dir_name + os_sep_rmt + file_name
                rc = ssh_command_with_rc("ls '" + file_path + "'")
                if rc == 0:
                    count_found += 1
                    print_log('D', 1, msg="Fichier existant.....: ", val=file_path, dotted=False)
                else:
                    count_notfound += 1
                    print_log('I', 1, msg="Fichier non-existant.: ", val=file_path, dotted=False)
                    dlt = db_h.cursor()
                    dlt.execute(delete, [dir_name, file_name, local_rmt])
    except sqlite3.Error as x:
        print_log('E', 0, msg="SQL Error: ", val=str(x), dotted=False)

    print_log('I', 0, msg="Nettoyage terminé")
    print_log('I', 1, msg="Fichiers trouvés", val=str(count_found))
    print_log('I', 1, msg="Fichiers manquants", val=str(count_notfound))
    print_log('I', 0)


def list_dup(db_h, root_dir):

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
        cur_md5 = db_h.cursor()
        cur_dup = db_h.cursor()
        for row_md5 in cur_md5.execute(sel_md5, [root_dir]):
            file_md5 = row_md5[0]
            print_log('I', 0, "Doublons possibles", val=file_md5)
            for row_dup in cur_dup.execute(sel_dup, [file_md5, root_dir]):
                dir_name = row_dup[0]
                file_name = row_dup[1]
                file_size = row_dup[2]
                file_mtime = row_dup[3]
                print_log('I', 1, msg="Fichier......: ", val=dir_name + os.sep + file_name, dotted=False)
                print_log('I', 2, msg="Size.....: ", val=str(file_size), dotted=False)
                print_log('I', 2, msg="MTime....: ", val=file_mtime, dotted=False)
            print_log('I', 0)
    except sqlite3.Error as x:
        print_log('E', 0, msg="SQL Error: ", val=str(x), dotted=False)


def find_missing_files(db_h, source_dir, target_dir):
    counts = {'copy': 0, 'compare': 0, 'kept': 0, 'newer': 0, 'older': 0}

    sel_src = \
        '''
        select dir_name, file_name, file_md5, file_size, file_mtime, rel_path
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
        cur_src = db_h.cursor()
        cur_tgt = db_h.cursor()
        for row_src in cur_src.execute(sel_src, [source_dir]):
            dir_name = row_src[0]
            file_name = row_src[1]
            file_md5_src = row_src[2]
            file_size_src = row_src[3]
            file_mtime_src = row_src[4]
            rel_path = row_src[5]
            cur_tgt.execute(sel_tgt, [target_dir, rel_path, file_name])
            row_tgt = cur_tgt.fetchone()
            if rel_path == '.':
                dir_name_tgt = target_dir
            else:
                dir_name_tgt = target_dir + os.sep + rel_path
            if parm["remote"] is None:
                local_rmt = 'L'
            else:
                local_rmt = 'R'
            new_file = File(file_name, file_md5_src, file_mtime_src, file_size_src, dir_name_tgt,
                            target_dir, rel_path, local_rmt)
            if row_tgt is None:
                # Copy
                rc = copy_file(dir_name, file_name, target_dir, rel_path)
                if rc == 0:
                    db_store_file(db_h, new_file)
                    counts['copy'] += 1
            else:
                counts['compare'] += 1
                file_md5_tgt = row_tgt[0]
                file_mtime_tgt = row_tgt[1]
                # Compare
                if file_md5_src == file_md5_tgt:
                    print_log('D', 0, msg="Le fichier n'a pas à être copié.")
                    counts['kept'] += 1
                else:
                    if file_mtime_src > file_mtime_tgt:
                        print_log('D', 0, msg="Le fichier est plus récent et doit être copié.")
                        rc = copy_file(dir_name, file_name, target_dir, rel_path)
                        if rc == 0:
                            db_store_file(db_h, new_file)
                            counts['newer'] += 1
                    else:
                        print_log('D', 0, msg="Le fichier sur la cible est plus récent. Il ne sera pas écrasé.")
                        counts['older'] += 1
    except sqlite3.Error as x:
        print_log('E', 0, msg="SQL Error: ", val=str(x), dotted=False)

    print_log('I', 0, msg="Statistiques pour les copies:")
    print_log('I', 1, msg="Fichiers copiés", val=str(counts['copy']))
    print_log('I', 1, msg="Comparaison requises", val=str(counts['compare']))
    print_log('I', 2, msg="Copies évitées (même checksum)", val=str(counts['kept']))
    print_log('I', 2, msg="Fichiers remplacés par un plus récent", val=str(counts['newer']))
    print_log('I', 2, msg="Fichiers cibles plus récents conservés", val=str(counts['older']))
    print_log('I', 0)
    return counts['copy'] + counts['newer']


def copy_file(dir_name, file_name, target_dir, rel_path):
    global ssh_client, ftp_client
    rc = 0
    print_log('D', 0, msg="Entrée dans copy_file")
    print_log('D', 1, msg="Dir Name", val=dir_name)
    print_log('D', 1, msg="File Name", val=file_name)
    print_log('D', 1, msg="Target Dir", val=target_dir)
    print_log('D', 1, msg="Relative path", val=rel_path)
    source_path = dir_name + os.sep + file_name
    print_log('I', 0, msg="Copie de", val=source_path)
    if parm['remote'] is None:
        if rel_path == '.':
            tgt_dir = target_dir
        else:
            tgt_dir = target_dir + os.sep + rel_path
        target_path = os.path.join(tgt_dir, file_name)
        print_log('I', 1, msg="vers", val=target_path)
        if parm["copy"]:
            os.makedirs(tgt_dir, exist_ok=True)
            shutil.copy2(source_path, target_path)
        else:
            print_log('I', 0, msg="Mode simulation: Fichier ne sera pas copié.")
            rc = 1
    else:
        if os.sep in rel_path:
            rel_path = rel_path.replace(os.sep, os_sep_rmt)
        if rel_path == '.':
            tgt_dir = target_dir
        else:
            tgt_dir = target_dir + os_sep_rmt + rel_path
        target_path = tgt_dir + os_sep_rmt + file_name
        print_log('I', 1, "vers", val="%s:%s" % (cred['host'], target_path))
        if parm["copy"]:
            dir_rc = check_target_dir_rmt(tgt_dir)
            if dir_rc == 0:
                ftp_success = False
                ftp_attempts = 1
                while not ftp_success and ftp_attempts <= 3:
                    try:
                        ftp_client.put(source_path, target_path)
                        ftp_success = True
                    except Exception as x:
                        print_log('E', 0, msg="SSH Error: ", val=str(x), dotted=False)
                        disconnect_ssh()
                        connect_ssh()
                    ftp_attempts += 1
                if ftp_attempts == 3:
                    print_log('E', 1, "La copie a échoué 3 fois.")
                    rc = 8
            else:
                print_log('E', 0, msg="Remote mkdir failed", val="RC=%i" % dir_rc)
                rc = 4
        else:
            print_log('I', 0, msg="Mode simulation: Fichier ne sera pas copié.")
            rc = 1
    return rc


def get_metadata(db_h, root_dir, dir_name, file_name):
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
    print_log('I', 0, msg="Fichier: ", val=file_path, dotted=False)
    print_log('D', 1, msg="Date modification (formatté)", val=file_mtime)
    print_log('D', 1, msg="Grosseur en bytes", val=str(file_size))
    print_log('D', 1, msg="Checksum", val=file_md5)
    file = File(file_name, file_md5, file_mtime, file_size, dir_name, root_dir, rel_path, "L")
    db_store_file(db_h, file)


def scan_dir(db_h, root_dir):
    print_log('D', 0, msg="Entrée dans scan_dir. Parm: ", val=root_dir, dotted=False)
    # Initialize counters
    accept_counts = {}
    for ext in config['accept_list']:
        accept_counts[ext] = 0
    reject_counts = {}
    for ext in config['reject_list']:
        reject_counts[ext] = 0
    others_counts = {}

    # Scan the directory structure
    print_log('I', 0, msg="Inspection de ", val=root_dir, dotted=False)
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            filename, file_ext = os.path.splitext(file)
            file_ext = file_ext.lower()
            if file_ext in config['accept_list']:
                accept_counts[file_ext] += 1
                get_metadata(db_h, root_dir, root, file)
            elif file_ext in config['reject_list']:
                reject_counts[file_ext] += 1
            else:
                if file_ext in others_counts:
                    others_counts[file_ext] += 1
                else:
                    others_counts[file_ext] = 1
                    print_log('I', 0, msg="Fichiers de type inconnu: ", val=os.path.join(root, file), dotted=False)

    # Summary Report
    print_log('I', 0)
    print_log('I', 0, msg="Statistiques pour ", val=root_dir, dotted=False)
    print_log('I', 1, msg="Comptes par type de fichiers acceptés:")
    for ext in config['accept_list']:
        print_log('I', 2, msg=ext, val=str(accept_counts[ext]))
    print_log('I', 1, msg="Comptes par type de fichiers rejetés:")
    for ext in config['reject_list']:
        print_log('I', 2, msg=ext, val=str(reject_counts[ext]))
    if len(others_counts) > 0:
        print_log('I', 1, msg="Comptes par type de fichiers inattendus:")
        for ext in others_counts:
            print_log('I', 2, msg=ext, val=str(others_counts[ext]))
    print_log('I', 0)
    print_log('D', 0, "Sortie de scan_dir.")


def scan_dir_rmt(db_h, root_dir):
    print_log('D', 0, msg="Entrée dans scan_dir_rmt. Parm: ", val=root_dir, dotted=False)

    count_files = 0
    accept_list = ",".join(config['accept_list'])
    reject_list = ",".join(config['reject_list'])
    print_log('D', 0, msg="Accept list: ", val=accept_list, dotted=False)
    print_log('D', 0, msg="Reject list: ", val=reject_list, dotted=False)

    print_log('I', 0, msg="Inspection de ", val=root_dir, dotted=False)
    command = "/home/jean/sync_rmt.py -s -d " + root_dir + " -a '" + accept_list + "' -r '" + reject_list + "'"
    stdin, stdout, stderr = ssh_client.exec_command(command)
    data = stdout.read().decode('utf-8')
    files = json.loads(data)
    for item in files:
        # dir_name, file_name, file_md5, file_mtime, file_size, root_dir, rel_path, local_rmt
        dir_name = item['dir']
        file_name = item['name']
        file_md5 = get_md5_rmt(dir_name, file_name)
        file_mtime = item['mtime']
        file_size = item['size']
        # root_dir from parm
        rel_path = item['rel_path']
        local_rmt = 'R'
        file = File(file_name, file_md5, file_mtime, file_size, dir_name, root_dir, rel_path, local_rmt)
        print_log('I', 0, msg="Fichier: " + cred['host'] + ":" + dir_name + os_sep_rmt + file_name)
        print_log('D', 0, msg=str(file))
        db_store_file(db_h, file)
        count_files += 1
    # Summary Report
    print_log('I', 0)
    print_log('I', 0, msg="Statistiques pour " + root_dir + ": " + str(count_files) + " fichiers.")
    print_log('I', 0)
    print_log('D', 0, "Sortie de scan_dir_rmt.")


def get_md5_rmt(dir_name, file_name):
    command = "/home/jean/sync_rmt.py -m -d '" + dir_name + "' -f '" + file_name + "'"
    stdin, stdout, stderr = ssh_client.exec_command(command)
    md5 = stdout.read().decode('utf-8')
    return md5


def connect_ssh():
    global ssh_client, ftp_client
    try:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(cred['host'], port=cred['port'], username=cred['user'], password=cred['pswd'])
        ftp_client = ssh_client.open_sftp()
    except Exception as x:
        print_log('E', 0, msg="SSH Error: ", val=str(x), dotted=False)
        ssh_client = None
        ftp_client = None
    return


def disconnect_ssh():
    global ssh_client, ftp_client
    if ftp_client is not None:
        ftp_client.close()
    if ssh_client is not None:
        ssh_client.close()


def ssh_command_with_rc(command):
    channel = ssh_client.get_transport().open_session()
    channel.exec_command(command)
    rc = channel.recv_exit_status()
    return rc


def main():
    global parm, config, conn, cred, ssh_client, ftp_client, os_sep_rmt

    # Get parameters and validate them
    (options, args) = parse_options()
    parm["log"] = options.log
    parm["copy"] = options.copy
    parm["remote"] = options.remote
    parm["dup"] = options.dup
    source_dir = args[0]
    target_dir = args[1]
    if parm['log'].upper() == 'CRITICAL':
        logging.basicConfig(level=logging.CRITICAL, format=' %(asctime)s - %(levelname)s - %(message)s')
    elif parm['log'].upper() == 'ERROR':
        logging.basicConfig(level=logging.ERROR, format=' %(asctime)s - %(levelname)s - %(message)s')
    elif parm['log'].upper() == 'WARNING':
        logging.basicConfig(level=logging.WARNING, format=' %(asctime)s - %(levelname)s - %(message)s')
    elif parm['log'].upper() == 'DEBUG':
        logging.basicConfig(level=logging.DEBUG, format=' %(asctime)s - %(levelname)s - %(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format=' %(asctime)s - %(levelname)s - %(message)s')

    print_log('I', 0, msg='Début du programme ', val=sys.argv[0], dotted=False)
    print_log('I', 0, msg='Paramètres:')
    print_log('I', 1, msg="Dossier source", val=source_dir)
    if not os.path.isdir(source_dir):
        print_log('E', 0, msg="Le dossier source n'existe pas.")
        return 8

    print_log('I', 1, msg="Dossier cible", val=target_dir)
    if parm["remote"] is not None:
        parse_host_info(parm["remote"])
        if not cred['valid']:
            return 8
        print_log('I', 1, msg="Le dossier cible est sur un serveur distant.")
        print_log('I', 2, msg="Serveur", val=cred['host'])
        print_log('I', 2, msg="Port", val=str(cred['port']))
        print_log('I', 2, msg="User", val=str(cred['user']))
        connect_ssh()
        os_sep_rmt = get_os_sep_rmt()
        print_log('I', 2, msg="Séparateur OS", val=os_sep_rmt)
        if check_target_dir_rmt(target_dir) > 0:
            return 8

    else:
        if not os.path.isdir(target_dir):
            print_log('W', 1, msg="Le dossier cible n'existe pas. Il sera créé.")
            os.makedirs(target_dir)

    if parm["copy"]:
        print_log('I', 1, msg="Option de copie", val="Oui")
    else:
        print_log('I', 1, msg="Option de copie", val="Non")

    parm["dup"] = parm["dup"].upper()
    if parm["dup"] == 'S':
        print_log('I', 1, msg="Option de vérification de doublons", val="Source")
    elif parm["dup"] == 'C':
        print_log('I', 1, msg="Option de vérification de doublons", val="Cible")
    elif parm["dup"] == 'T':
        print_log('I', 1, msg="Option de vérification de doublons", val="Tous")
    elif parm["dup"] == 'N':
        print_log('I', 1, msg="Option de vérification de doublons", val="Non")
    else:
        print_log('I', 1, msg="Option de vérification de doublons", val="Invalide")
    print_log('I', 0, msg=" ")

    # Read config file
    config = parse_configs()
    print_log('I', 0, msg="Configurations:")
    print_log('I', 1, msg="Fichier de configuration", val=CONFIG_FILE)
    for ext in config['accept_list']:
        print_log('I', 1, msg="Accepted extension", val=ext)
    for ext in config['reject_list']:
        print_log('I', 1, msg="Rejected extension", val=ext)

    db_name = db_get_name(source_dir, target_dir, cred['host'])
    db_path = source_dir + os.sep + db_name
    print_log('I', 1, msg="Database file", val=db_name)
    print_log('I', 0, msg=" ")

    conn = sqlite3.connect(db_path)
    db_create_tables(conn)               # Create the DB objects
    db_remove_deleted(conn)  # Remove deleted files from db
    scan_dir(conn, source_dir)           # Create inventory of the files in the source directory structure
    conn.commit()

    if parm["dup"] in 'ST':
        list_dup(conn, source_dir)

    if parm['remote'] is None:
        scan_dir(conn, target_dir)      # Create inventory of the files in the target directory structure
    else:
        scan_dir_rmt(conn, target_dir)
    conn.commit()

    find_missing_files(conn, source_dir, target_dir)  # Identify files that need to be copied

    if parm["dup"] in 'CT':
        list_dup(conn, target_dir)

    conn.commit()
    conn.close()

    disconnect_ssh()

    print_log('I', 0, msg="Fin du programme", val=sys.argv[0], dotted=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
