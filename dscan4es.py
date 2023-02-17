#!/usr/bin/python
# -*- coding: UTF-8 -*-
import re
import http.client
import json
import logging
import sys
import time
import argparse
import datetime
import os
import stat
import pwd
import grp
import pprint
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

BulkOpBuffer = ''
BulkOpBufferSize = 0
BulkOpBufferMaxSize = 1000
ToBeIgnoredList = ['.snapshot']
UID_list = {}
GID_list = {}
Conn = {}
SSLConext = {}

es_url = "elasticsearch-6-122.isilon.com"
es_port = "9200"
index_name = "isilon2"
bulk_url = "/" + index_name + "/_bulk/"
pw_str = 'ZWxhc3RpYzpkMVZER3ZfUzk3WE5oRFdYbWRlbA=='
logging_level = logging.WARN
path = '/mnt/'

def init():
    global UID_list
    global GID_list
    global Conn
    global SSLConext
    
    pwd_list = pwd.getpwall()
    for p in pwd_list:
        UID_list[p.pw_uid] = p.pw_name
    
    grp_list = grp.getgrall()
    for g in grp_list:
        GID_list[g.gr_gid] = g.gr_name

    try:
        Conn = http.client.HTTPSConnection(es_url, es_port, timeout=60)
    except http.client.HTTPException as e:
        logging.error("Failed to connect " + es_url + ":" + str(e))
        exit(255)

def scan_and_update(path, maxdepth):
    global ToBeIgnoredList
    total_entry_number_including_subdir = 0
    total_entry_size_including_subdir = 0
    total_entry_number = 0
    total_entry_size = 0
    total_file_number = 0
    total_dir_number = 0
    total_softlink_number = 0
    d_total_entry_number_including_subdir = 0
    d_total_entry_size_including_subdir = 0
    d_total_entry_number = 0
    d_total_entry_size = 0
    d_total_file_number = 0
    d_total_dir_number = 0
    d_total_softlink_number = 0
    maxdepth -= 1
    
    for entry in os.scandir(path):
        if (entry.name in ToBeIgnoredList):
            continue
        try:
            entry_stat = entry.stat()
        except OSError as e:
            logging.warn("Failed to stat {} : {}".format(entry.path, str(e)))
            continue
        owner_name,group_name = get_name_by_id(entry_stat.st_uid, entry_stat.st_gid)
        ftype,permission = get_type_permission(entry_stat.st_mode)
        directory_path = get_directory_path(entry.path)
        ext = get_file_ext(entry.name)
        
        if (ftype == 'directory' and maxdepth > 0):
            d_total_entry_number_including_subdir, d_total_entry_size_including_subdir, d_total_entry_number, d_total_entry_size, d_total_file_number, d_total_dir_number, d_total_softlink_number = scan_and_update(entry.path, maxdepth)
            total_entry_number_including_subdir = total_entry_number_including_subdir + d_total_entry_number_including_subdir
            total_entry_size_including_subdir = total_entry_size_including_subdir + d_total_entry_size_including_subdir
        
        total_entry_number += 1        
        total_entry_size = total_entry_size + entry_stat.st_size

        db_total_entry_number_including_subdir = 0
        db_total_entry_size_including_subdir = 0
        db_total_entry_number = 0
        db_total_entry_size = 0
        db_total_file_number = 0
        db_total_dir_number = 0
        db_total_softlink_number = 0
        if (ftype == 'directory'):
            total_dir_number +=1
            db_total_entry_number_including_subdir = d_total_entry_number_including_subdir
            db_total_entry_number = d_total_entry_number
            db_total_entry_size = d_total_entry_size
            db_total_file_number = d_total_file_number
            db_total_dir_number = d_total_dir_number
            db_total_softlink_number = d_total_dir_number
        if (ftype == 'softlink'):
            total_softlink_number +=1
        if (ftype == 'file'):
            total_file_number +=1

        update_db(entry_stat.st_ino,{
            "name": entry.name,
            "ext": ext,
            "path": entry.path,
            "directory_path": directory_path,
            "mtime": datetime.datetime.fromtimestamp(entry_stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
            "atime": datetime.datetime.fromtimestamp(entry_stat.st_atime).strftime('%Y-%m-%d %H:%M:%S'),
            "ctime": datetime.datetime.fromtimestamp(entry_stat.st_ctime).strftime('%Y-%m-%d %H:%M:%S'),
            "uid": entry_stat.st_uid,
            "gid": entry_stat.st_gid,
            "owner": owner_name,
            "group": group_name,
            "size": entry_stat.st_size,
            "type": ftype,
            "permission": permission,
            "total_entry_number_including_subdir": db_total_entry_number_including_subdir,
            "total_entry_size_including_subdir": db_total_entry_size_including_subdir,
            "total_entry_number": db_total_entry_number,
            "total_entry_size": db_total_entry_size,
            "total_file_number": db_total_file_number,
            "total_dir_number": db_total_dir_number,
            "total_softlink_number": db_total_softlink_number
            })
    total_entry_number_including_subdir = total_entry_number_including_subdir + total_entry_number
    total_entry_size_including_subdir = total_entry_size_including_subdir + total_entry_size
    return total_entry_number_including_subdir, total_entry_size_including_subdir, total_entry_number, total_entry_size, total_file_number, total_dir_number, total_softlink_number

def get_directory_path(path):
    directory_path_index = path.rfind('/')
    if( directory_path_index >= 0) :
        return path[:directory_path_index]
    else:
        return ''
    
    
def get_file_ext(name):
    ext_index = name.rfind('.')
    if( ext_index >= 0) :
        return name[ext_index+1:]
    else:
        return ''

def update_db(inode, fileinfo):
    global BulkOpBuffer
    global BulkOpBufferSize
    global BulkOpBufferMaxSize
    
    if ( BulkOpBufferSize > BulkOpBufferMaxSize):
        es_bulk_create()
    else:
        cs = '{"create": {"_id": "' + str(inode) + '"}}\n'
        js = json.dumps(fileinfo) + '\n'
        BulkOpBuffer = BulkOpBuffer + cs + js 
        BulkOpBufferSize += 1
    
def es_bulk_create():
    global BulkOpBuffer
    global BulkOpBufferSize
    global Conn
    
    headers = {'Authorization': 'Basic ' + pw_str, 'Content-Type': 'application/json'}
    Conn.request("POST", bulk_url, BulkOpBuffer, headers)
    res = Conn.getresponse()
    data = res.read()
    if (res.status != 200):
        logging.warn("Failed to process bulk operation. Error message: {}".format(data.decode("utf-8")))
    logging.debug(data.decode("utf-8"))
    
    logging.debug(BulkOpBuffer)

    BulkOpBuffer = ''
    BulkOpBufferSize = 0

def get_name_by_id(uid,gid):
    global UID_list
    global GID_list
    
    if (uid in UID_list):
        user_name = UID_list[uid]
    else:
        user_name = str(uid)
        
    if (gid in GID_list):
        group_name = GID_list[gid]
    else:
        group_name = str(gid)
        
    return user_name, group_name

def get_type_permission(st_mode):
    mode_str = stat.filemode(st_mode)
    ftype='file'
    if(mode_str[0] == 'd'):
        ftype='directory'
    if(mode_str[0] == 'l'):
        ftype='softlink'
    return ftype, mode_str[1:]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scan mount point and ingest file list to Elastic Search')
    # requiredargs = parser.add_argument_group('required arguments')
    # requiredargs.add_argument('--path', '-p', help='The path of mount point to be scanned', required=True)
    # requiredargs.add_argument('--es_url', '-e', help='The url of ES server.', required=True)
    # requiredargs.add_argument('--es_port', '-p', help='The port of ES server.', required=True)
    # requiredargs.add_argument('--index', '-i', help='The index name.', required=True)
    parser.add_argument('--log', '-l', help='The output log file name. Output to screen if not specified.')
    args = parser.parse_args()
    log_file_name = args.log

    if (args.log is not None):
        logging.basicConfig(
            filename=args.log,
            format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
            datefmt="%d-%m-%Y %H:%M:%S",
            level=logging_level)
    else:
        logging.basicConfig(
            stream=sys.stdout,
            format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
            datefmt="%d-%m-%Y %H:%M:%S",
            level=logging_level)
    
    init()
    total_entry_number_including_subdir, total_entry_number, total_file_number, total_dir_number, total_softlink_number = scan_and_update(path,100)

    es_bulk_create()

    print(total_entry_number_including_subdir)
    

