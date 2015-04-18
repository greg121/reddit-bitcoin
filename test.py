#!/usr/bin/python
# -*- coding: utf-8 -*-

import re
import sys
sys.path.insert(0, '/home/gregor/reddit/senti')
import sentistrength
import dbhelper

def main():
    db = dbhelper.connectToDb("mysql")
    cur = db.cursor()
    cur.execute("SELECT * FROM `submissions`")
    numrows = cur.rowcount
    return numrows

if __name__ == '__main__':
    main()
