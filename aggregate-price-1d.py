#!/usr/bin/python
# -*- coding: utf-8 -*-

import dbhelper
from datetime import timedelta, datetime

#datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S') um in datetime umzuwandeln
def addToDatabase(db, cur, timestamp, open_, high, low, close, volume):
    sql = "INSERT INTO `bitcoin-price-1d`(`id`, `timestamp`, `open`, `high`, `low`, `close`, `volume`) \
    VALUES (NULL, '%s', '%s', '%s', '%s', '%s', '%s')" % \
    (timestamp, open_, high, low, close, volume)
    cur.execute(sql)
    db.commit()

def queryPrices(db, cur, start):
    sql = "SELECT * FROM `bitcoin-price-complete` WHERE `timestamp` BETWEEN '%i' AND '%i'" % \
    (start, start+86400)
    cur.execute(sql)
    db.commit()
    return cur

def main():
    #1325376000 = 2012-01-01 00:00:00
    #1390950000 = 2014-01-29 00:00:00
    start = 1325376000
    db = dbhelper.connectToDb('mysql')
    cur = db.cursor()
    #for x in range(50):
    for x in range(1200):
        print start
        cur = queryPrices(db, cur, start)
        result = cur.fetchall()
        priceArray = []
        volume = 0

        if (cur.rowcount != 0):
            for y in range(int(cur.rowcount)):
                priceArray.append(result[y][2])
                volume = volume + result[y][3]
            openPrice = priceArray[0]
            highPrice = max(priceArray)
            lowPrice = min(priceArray)
            closePrice = priceArray[len(priceArray)-1]

            addToDatabase(db, cur, start, openPrice, highPrice, lowPrice, closePrice, volume)
            start = start + 86400
            #summer time: at 1396134000 calculate 1h less
            #if (start == 1396393200):
            #    start = start + 601200
            #else:
            #    start = start + 604800

        else:
            #für zwischendurch, wenn daten fehlen
            addToDatabase(db, cur, start, openPrice, highPrice, lowPrice, closePrice, volume)
            start = start + 86400
    db.close()

if __name__ == '__main__':
    main()
