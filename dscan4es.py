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
import base64
ssl._create_default_https_context = ssl._create_unverified_context

BulkOpBuffer = ''
BulkOpBufferSize = 0
BulkOpBufferMaxSize = 1000
ToBeIgnoredList = ['.snapshot']
UID_list = {}
GID_list = {}
Conn = {}
SslContext = {}

EsUrl = "elasticsearch-6-122.isilon.com"
EsPort = "9200"
IndexName = "isilon1"
EsUser = 'elastic'
EsPassword = 'd1VDGv_S97XNhDWXmdel'

PwStr = ''
Logging_Level = logging.DEBUG

def init():
    global UID_list
    global GID_list
    global Conn
    global SslContext
    global PwStr
    
    pwd_list = pwd.getpwall()
    for p in pwd_list:
        UID_list[p.pw_uid] = p.pw_name
    
    grp_list = grp.getgrall()
    for g in grp_list:
        GID_list[g.gr_gid] = g.gr_name

    try:
        Conn = http.client.HTTPSConnection(EsUrl, EsPort, timeout=60)
    except http.client.HTTPException as e:
        logging.error("Failed to connect " + EsUrl + ":" + str(e))
        exit(255)
    
    PwStr = base64.b64encode('{}:{}'.format(EsUser, EsPassword).encode()).decode()

def scan_and_update(path, maxdepth, smart_scan, is_changed):
    global ToBeIgnoredList
    total_entry_number_including_subdir = 0
    total_entry_size_including_subdir = 0
    total_entry_number = 0
    total_entry_size = 0

    d_total_entry_number_including_subdir = 0
    d_total_entry_size_including_subdir = 0
    d_total_entry_number = 0
    d_total_entry_size = 0

    maxdepth -= 1
    
    for entry in os.scandir(path):
        if (entry.name in ToBeIgnoredList):
            continue
            
        total_entry_number += 1        
        total_entry_size = total_entry_size + entry_stat.st_size
        if (not is_changed and not entry.is_dir())
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
            d_total_entry_number_including_subdir, d_total_entry_size_including_subdir, d_total_entry_number, d_total_entry_size = scan_and_update(entry.path, maxdepth, smart_scan, check_dir(entry_stat.st_ino, entry_stat.st_mtime))
            
            total_entry_number_including_subdir = total_entry_number_including_subdir + d_total_entry_number_including_subdir
            
            total_entry_size_including_subdir = total_entry_size_including_subdir + d_total_entry_size_including_subdir
        


        db_total_entry_number_including_subdir = 0
        db_total_entry_size_including_subdir = 0
        db_total_entry_number = 0
        db_total_entry_size = 0

        if (ftype == 'directory'):
            total_dir_number +=1
            db_total_entry_number_including_subdir = d_total_entry_number_including_subdir
            db_total_entry_number = d_total_entry_number
            db_total_entry_size = d_total_entry_size


        update_db(entry_stat.st_ino,{
            "name": entry.name,
            "ext": ext,
            "path": entry.path,
            "directory_path": directory_path,
            "mtime": datetime.datetime.fromtimestamp(entry_stat.st_mtime).strftime('%Y-%m-%d'),
            "st_mtime": entry_stat.st_mtime,
            "atime": datetime.datetime.fromtimestamp(entry_stat.st_atime).strftime('%Y-%m-%d'),
            "st_atime": entry_stat.st_atime,
            "ctime": datetime.datetime.fromtimestamp(entry_stat.st_ctime).strftime('%Y-%m-%d'),
            "st_ctime": entry_stat.st_ctime,
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
            "total_entry_size": db_total_entry_size
            })
    total_entry_number_including_subdir = total_entry_number_including_subdir + total_entry_number
    total_entry_size_including_subdir = total_entry_size_including_subdir + total_entry_size
    return total_entry_number_including_subdir, total_entry_size_including_subdir, total_entry_number, total_entry_size

def check_dir(inode, mtime):
    return True

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
    global PwStr
    
    bulk_url = "/" + IndexName + "/_bulk/"
        
    headers = {'Authorization': 'Basic ' + PwStr, 'Content-Type': 'application/json'}
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
    #global EsUrl
    #global EsPort
    #global IndexName
    #global EsUser
    #global EsPassword
    
    parser = argparse.ArgumentParser(description='Scan mount point and ingest file list to Elastic Search')
    requiredargs = parser.add_argument_group('required arguments')
    # requiredargs.add_argument('--path', '-p', help='The path of mount point to be scanned', required=True)
    # requiredargs.add_argument('--es_url', '-e', help='The url of ES server.', required=True)
    # requiredargs.add_argument('--es_port', '-o', help='The port of ES server.', required=True)
    # requiredargs.add_argument('--index', '-i', help='The index name.', required=True)
    # requiredargs.add_argument('--maxdepth', '-m', help='The maximum level of directory to be scanned.', required=True)
    # requiredargs.add_argument('--es_user', '-u', help='The ES user.', required=True)
    # requiredargs.add_argument('--es_password', '-w', help='The ES password.', required=True)
    parser.add_argument('--log', '-l', help='The output log file name. Output to screen if not specified.')
    
    args = parser.parse_args()
    log_file_name = args.log
    # EsUrl = args.EsUrl
    # EsPort = args.es_password
    # IndexName = args.index
    # EsUser = args.es_user
    # EsPassword = args.es_password

    if (args.log is not None):
        logging.basicConfig(
            filename=args.log,
            format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
            datefmt="%d-%m-%Y %H:%M:%S",
            level=Logging_Level)
    else:
        logging.basicConfig(
            stream=sys.stdout,
            format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
            datefmt="%d-%m-%Y %H:%M:%S",
            level=Logging_Level)
    
    init()

    # total_entry_number_including_subdir, total_entry_size_including_subdir, total_entry_number, total_entry_size, total_file_number, total_dir_number, total_softlink_number = scan_and_update(args.path, args.maxdepth)
    total_entry_number_including_subdir, total_entry_size_including_subdir, total_entry_number, total_entry_size = scan_and_update('/zx/test', 10, True, True)

    es_bulk_create()

    print(total_entry_number_including_subdir)
    

