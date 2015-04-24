#!/usr/bin/python
# -*- coding: utf-8 -*-

import dbhelper
from datetime import timedelta, datetime
import numpy as np

def addToDatabase(db, cur, date, sentiment):
    sql = "INSERT INTO `reddit-sentiment`(`id`, `date`, `sentiment`) \
    VALUES (NULL, '%s', '%d')" % (date, sentiment)
    cur.execute(sql)
    db.commit()

def querySentiment(db, cur, start):
    sql = "SELECT `score` FROM `submissions_copy` WHERE `date` BETWEEN '%s' AND '%s'" % \
    (start, str(datetime.strptime(start, '%Y-%m-%d %H:%M:%S') + timedelta(days=1)))
    cur.execute(sql)
    db.commit()
    return cur

def main():
    #2011-09-15 00:00:00 = 1316041200
    start = "2012-07-13 00:00:00"
    #start = "2014-03-23 08:00:00"
    db = dbhelper.connectToDb('mysql')
    cur = db.cursor()
    for x in range(1500):
        print start
        cur = querySentiment(db, cur, start)
        result = cur.fetchall()
        sentiment = 0
        for y in range(int(cur.rowcount)):
            sentiment = sentiment + result[y][0]
        print sentiment
        addToDatabase(db, cur, start, sentiment)
        start = str(datetime.strptime(start, '%Y-%m-%d %H:%M:%S') + timedelta(days=1))


if __name__ == '__main__':
    main()
