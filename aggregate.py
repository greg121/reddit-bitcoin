#!/usr/bin/python
# -*- coding: utf-8 -*-

import dbhelper

def addToDatabase(db, cur, timestamp, price, volume):
    sql = "INSERT INTO `bitcoin-price-5min`(`id`, `timestamp`, `price`, `volume`) \
    VALUES (NULL, '%s', '%s', '%s')" % \
    (timestamp, price, volume)
    cur.execute(sql)
    db.commit()

def queryPrices(db, cur, start):
    sql = "SELECT * FROM `bitcoin-price` WHERE `timestamp` BETWEEN '%i' AND '%i'" % \
    (start, start+300)
    cur.execute(sql)
    db.commit()
    return cur

def main():
    start = 1315922000
    db = dbhelper.connectToDb('mysql')
    cur = db.cursor()
    while 1:
        cur = queryPrices(db, cur, start)
        result = cur.fetchall()
        if (cur.rowcount != 0):
            print result[int(cur.rowcount)-1][1]
            addToDatabase(db, cur, start, result[int(cur.rowcount)-1][2], result[int(cur.rowcount)-1][3])
            start = start + 300
        else:
            break
    db.close()

if __name__ == '__main__':
    main()
