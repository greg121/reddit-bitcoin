#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
import ConfigParser

#
def connectToDb(string):
    # config einlesen
    config = ConfigParser.RawConfigParser()
    config.read('config.ini')

    # quelle: http://stackoverflow.com/a/622308/1018288
    db = MySQLdb.connect(host=config.get(string, 'host'),
                         user=config.get(string, 'user'),
                         passwd=config.get(string, 'passwd'),
                         db=config.get(string, 'db'),
                         charset="utf8")

    return db
