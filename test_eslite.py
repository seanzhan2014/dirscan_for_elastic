#!/usr/bin/python
# -*- coding: UTF-8 -*-
import eslite


EsUrl = "elasticsearch-6-122.isilon.com"
EsPort = "9200"
IndexName = "isilon1"
EsUser = 'elastic'
EsPassword = 'd1VDGv_S97XNhDWXmdel'

e = eslite.es(10, EsUrl, EsPort, EsUser, EsPassword, IndexName)
fileinfo = {'abc':'efg'}
e.add_entry('a', 123, fileinfo)