#!/usr/bin/python
# -*- coding: utf-8 -*-

import dbhelper
from datetime import timedelta, datetime

def addToDatabase(db, cur, date, sentiment):
    sql = "INSERT INTO `bitcointalk-sentiment-1h`(`id`, `date`, `sentiment`) \
    VALUES (NULL, '%s', '%d')" % (date, sentiment)
    cur.execute(sql)
    db.commit()

def querySentiment(db, cur, start):
    sql = "SELECT `eng_pos`, `eng_neg` FROM `forummsg` WHERE `date` BETWEEN '%s' AND '%s'" % \
    (start, str(datetime.strptime(start, '%Y-%m-%d %H:%M:%S') + timedelta(hours=1)))
    cur.execute(sql)
    db.commit()
    return cur

def main():
    #2011-09-15 00:00:00 = 1316041200
    start = "2014-01-29 00:00:00"
    server1und1 = dbhelper.connectToDb('mysql')
    profdell = dbhelper.connectToDb('bitcointalk')
    cur_1und1 = server1und1.cursor()
    cur_prof = profdell.cursor()
    for x in range(6240):
        print x
        cur = querySentiment(profdell, cur_prof, start)
        result = cur.fetchall()
        sentiment = 0
        for y in range(int(cur.rowcount)):
            sentiment = sentiment + result[y][0] + result[y][1]
        addToDatabase(server1und1, cur_1und1, start, sentiment)
        start = str(datetime.strptime(start, '%Y-%m-%d %H:%M:%S') + timedelta(hours=1))


if __name__ == '__main__':
    main()
