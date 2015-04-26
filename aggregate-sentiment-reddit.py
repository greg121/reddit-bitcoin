#!/usr/bin/python
# -*- coding: utf-8 -*-

import dbhelper
from datetime import timedelta, datetime
import numpy as np

def addToDatabase(db, cur, date, sentiment, avg, weighted_avg):
    sql = "INSERT INTO `reddit-sentiment`(`id`, `date`, `sentiment`, `avg`, `weighted_avg`) \
    VALUES (NULL, '%s', '%d', '%f', '%f')" % (date, sentiment, avg, weighted_avg)
    cur.execute(sql)
    db.commit()

def querySentiment(db, cur, start):
    sql = "SELECT `score`, `upvotes` FROM `submissions_copy` WHERE `date` BETWEEN '%s' AND '%s'" % \
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
    for x in range(837):
        print start
        cur = querySentiment(db, cur, start)
        result = cur.fetchall()
        sentiment = 0
        weighted_sentiment = 0
        list = []
        weighted_list = []
        for y in range(int(cur.rowcount)):
            sentiment = sentiment + result[y][0]
            weighted_list.append(result[y][0]*result[y][1])
            list.append(result[y][0])
        print sentiment
        avg = np.mean(list)
        weighted_avg = np.mean(weighted_list)
        print weighted_avg
        list = []
        weighted_list = []
        addToDatabase(db, cur, start, sentiment, avg, weighted_avg)
        start = str(datetime.strptime(start, '%Y-%m-%d %H:%M:%S') + timedelta(days=1))


if __name__ == '__main__':
    main()
