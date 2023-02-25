#!/usr/bin/python
# -*- coding: UTF-8 -*-
''' This is a light weight SDK for elastic search via Restful API.
It's designed for file system analytics.
'''
import re
import http.client
import json
import sys
import time
import datetime
import os
import stat
import pwd
import grp
import pprint
import ssl
import base64
ssl._create_default_https_context = ssl._create_unverified_context
class es:
    __buffer = ''
    __buffer_size = 0
    __max_buff_size = 1000
    __conn = {}
    __pw_token = ''
    __index = ''
    __uid_list = {}
    __gid_list = {}

    def __init__(self, url, port, user, password, index, max_buffer_size = 1000):
        self.__max_buff_size = max_buffer_size
        self.__pw_token = base64.b64encode('{}:{}'.format(user, password).encode()).decode()
        self.__conn = http.client.HTTPSConnection(url, port, timeout=60)
        self.__index = index
        
        pwd_list = pwd.getpwall()
        for p in pwd_list:
            self.__uid_list[p.pw_uid] = p.pw_name
        
        grp_list = grp.getgrall()
        for g in grp_list:
            self.__gid_list[g.gr_gid] = g.gr_name
                
    def __del__(self):
        self.flush_buffer()
    
    def add_entry(self, entry, entry_stat, additional_info):
        ''' Add file system entry information to elastic search.
        parameters:
            entry: file system entry info, returned by os.scandir()
            entry_stat: file system entry stat, returned by os.scandir().stat(), or os.stat()
            additional_info: additional info to be put into elastic search. It has to be a dictionary
        '''
  
        inode, info = self.__parse_fs_entry(entry, entry_stat)  
        cs = '{"create": {"_id": "' + str(inode) + '"}}\n'
        js = json.dumps(info) + '\n'
        self.__buffer = self.__buffer + cs + js 
        self.__buffer_size += 1
        if self.__buffer_size > self.__max_buff_size :
            self.flush_buffer()
    
    def __parse_fs_entry(self, entry, entry_stat):
        owner_name,group_name = self.__get_name_by_id(entry_stat.st_uid, entry_stat.st_gid)
        ftype,permission = self.__get_type_permission(entry_stat.st_mode)
        directory_path = self.__get_directory_path(entry.path)
        ext = self.__get_file_ext(entry.name)
        
        info = {
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
            "permission": permission
            }
        return entry_stat.st_ino, info
    
    def __get_directory_path(self, path):
        directory_path_index = path.rfind('/')
        if( directory_path_index >= 0) :
            return path[:directory_path_index]
        else:
            return ''
    
    
    def __get_file_ext(self, name):
        ext_index = name.rfind('.')
        if( ext_index >= 0) :
            return name[ext_index+1:]
        else:
            return ''
    
    def __get_name_by_id(self, uid, gid):
        if (uid in self.__uid_list):
            user_name = self.__uid_list[uid]
        else:
            user_name = str(uid)
            
        if (gid in self.__gid_list):
            group_name = self.__gid_list[gid]
        else:
            group_name = str(gid)
            
        return user_name, group_name

    def __get_type_permission(self, st_mode):
        mode_str = stat.filemode(st_mode)
        ftype='file'
        if(mode_str[0] == 'd'):
            ftype='directory'
        if(mode_str[0] == 'l'):
            ftype='softlink'
        return ftype, mode_str[1:]
    
    def is_index_exist(self):
        pass
        
    def create_index(self):
        pass
        
    def is_entry_exist(self):
        pass
        
    def delete_entries(self):
        pass
        
    def delete_directory_tree(self):
        pass
        
    def delete_directory_layer(self):
        pass
        
    def flush_buffer(self):
        status, data = self.__post(self.__buffer)
        
        self.__buffer = ''
        self.__buffer_size = 0
        
        if (status != 200):
            raise EsOpError("Failed to process bulk operation. Error message: {}".format(data.decode("utf-8")))
    
    def __post(self, msg):
        bulk_url = "/" + self.__index + "/_bulk/"        
        headers = {'Authorization': 'Basic ' + self.__pw_token, 'Content-Type': 'application/json'}
        self.__conn.request("POST", bulk_url, msg, headers)
        res = self.__conn.getresponse()
        data = res.read()
        return res.status, data


class EsOpError(Exception):
    def __init__(self, error_msg):
        self.message = error_msg
        super().__init__(self.message)
        
        
        
        
        
        
        
        
        
        