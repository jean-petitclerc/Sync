#!/usr/bin/python3

import os
import sys
import time
import logging
import hashlib
import json
from optparse import OptionParser

logging.basicConfig(level=logging.ERROR, format=' %(asctime)s - %(levelname)s - %(message)s')


# parms
parm = {'scan': False, 'md5': False}


def parse_options():
    # Use optparse to get parms
    usage = "usage: %prog [options] "
    parser = OptionParser(usage=usage)
    parser.add_option("-s", "--scan", dest="scan", action="store_true", default=False,
                      help="Scan a directory and produce a json file. Require -d, -a and -r")
    parser.add_option("-m", "--md5", dest="md5", action="store_true", default=False,
                      help="Compute a md5 checksum. Require: -d and -f")
    parser.add_option("-a", "--accept", dest="accept", action="store", default=False,
                      help="The list of file extensions to accept.")
    parser.add_option("-r", "--reject", dest="reject", action="store", default=False,
                      help="The list of file extensions to reject.")
    parser.add_option("-d", "--dir", dest="dir_name", action="store", default=False,
                      help="The directory name.")
    parser.add_option("-f", "--file", dest="file_name", action="store", default=False,
                      help="The file name.")
    parser.add_option("-o", "--ossep", dest="os_sep", action="store_true", default=False,
                      help="Return the os.sep")
    (options, args) = parser.parse_args()
    return options, args  # options: scan, md5; args(None)


def get_metadata(root_dir, dir_name, file_name):
    file_path = os.path.join(dir_name, file_name)
    (mode, ino, dev, nlink, uid, gid, file_size, atime, mtime, ctime) = os.stat(file_path)
    lastmod_date = time.localtime(mtime)
    file_mtime = time.strftime("%Y-%m-%d-%H.%M.%S", lastmod_date)
    rel_path = os.path.relpath(dir_name, root_dir)
    return rel_path, file_size, file_mtime


def get_md5(dir_name, file_name):
    # Open,close, read file and calculate MD5 on its contents
    file_path = os.path.join(dir_name, file_name)
    with open(file_path, "rb") as file_to_check:
        # read contents of the file
        data = file_to_check.read()
        # pipe contents of the file through
        file_md5 = hashlib.md5(data).hexdigest()
    return file_md5


def scan_dir(root_dir, accept_list, reject_list):
    result = []
    # Initialize counters
    accept_counts = {}
    for ext in accept_list:
        accept_counts[ext] = 0
    reject_counts = {}
    for ext in reject_list:
        reject_counts[ext] = 0
    others_counts = {}

    # Scan the directory structure
    logging.debug("Inspection de " + root_dir)
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            filename, file_ext = os.path.splitext(file)
            file_ext = file_ext.lower()
            if file_ext in accept_list:
                accept_counts[file_ext] += 1
                rel_path, file_size, file_mtime = get_metadata(root_dir, root, file)
                file_item = {'dir': root, 'name': file, 'rel_path': rel_path, 'size': file_size, 'mtime': file_mtime}
                result.append(file_item)
            elif file_ext in reject_list:
                reject_counts[file_ext] += 1
            else:
                if file_ext in others_counts:
                    others_counts[file_ext] += 1
                else:
                    others_counts[file_ext] = 1
                    logging.debug("Fichiers de type inconnu: " + os.path.join(root, file))

    # Summary Report
    logging.debug(" ")
    logging.debug("Statistiques pour " + root_dir)
    logging.debug("    Comptes par type de fichiers acceptés:")
    for ext in accept_list:
        logging.debug(("        " + ext).ljust(49, '.') + ": %i" % accept_counts[ext])
    logging.debug("    Comptes par type de fichiers rejetés:")
    for ext in reject_list:
        logging.debug(("        " + ext).ljust(49, '.') + ": %i" % reject_counts[ext])
    if len(others_counts) > 0:
        logging.debug("    Comptes par type de fichiers inattendus:")
        for ext in others_counts:
            logging.debug(("        " + ext).ljust(49, '.') + ": %i" % others_counts[ext])
    logging.debug(" ")
    logging.debug("Sortie de scan_dir.")
    return result
  
    
def main():
    # Get parameters and validate them
    (options, args) = parse_options()
    parm["scan"] = options.scan
    parm["md5"] = options.md5
    parm["os_sep"] = options.os_sep
    if parm["scan"]:
        if not options.dir_name:
            logging.error("-d is required for the scan option")
        dir_name = options.dir_name
        if not options.accept:
            logging.error("-a is required for the scan option")
        accept_list = options.accept.split(',')
        if not options.reject:
            logging.error("-r is required for the scan option")
        reject_list = options.reject.split(',')
        result = scan_dir(dir_name, accept_list, reject_list)
        print(json.dumps(result, indent=4))
    elif parm["md5"]:
        if not options.dir_name:
            logging.error("-d is required for the md5 option")
        dir_name = options.dir_name
        if not options.file_name:
            logging.error("-f is required for the md5 option")
        file_name = options.file_name
        md5 = get_md5(dir_name, file_name)
        print(md5)
    elif parm["os_sep"]:
        print(os.sep)
    else:
        logging.error("Invalid option")


if __name__ == "__main__":
    sys.exit(main())
