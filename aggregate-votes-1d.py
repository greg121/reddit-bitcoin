#!/usr/bin/python
# -*- coding: utf-8 -*-

import dbhelper
from datetime import timedelta, datetime

def addToDatabase(db, cur, timestamp, votes):
    sql = "INSERT INTO `votes-1d`(`id`, `timestamp`, `votes`) \
    VALUES (NULL, '%s', '%d')" % (timestamp, votes)
    cur.execute(sql)
    db.commit()

def queryVotes(db, cur, start):
    sql = "SELECT * FROM `submissions` WHERE `timestamp` BETWEEN '%i' AND '%i'" % \
    (start, start+86400)
    cur.execute(sql)
    db.commit()
    return cur

def main():
    #1325376000 = 01.01.2012
    start = 1325376000
    db = dbhelper.connectToDb('mysql')
    cur = db.cursor()
    cur = queryVotes(db, cur, start)
    while (cur.rowcount != 0):
        votes = 0
        print start
        result = cur.fetchall()
        for y in range(int(cur.rowcount)):
            votes = votes + result[y][6]
        addToDatabase(db, cur, start, votes)
        start = start + 86400
        queryVotes(db, cur, start)
    db.close()

if __name__ == '__main__':
    main()
