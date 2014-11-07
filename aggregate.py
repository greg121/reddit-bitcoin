#!/usr/bin/python
# -*- coding: utf-8 -*-

import dbhelper

def addToDatabase(db, cur, timestamp, open_, high, low, close, volume):
    sql = "INSERT INTO `bitcoin-price-15min`(`id`, `timestamp`, `open`, `high`, `low`, `close`, `volume`) \
    VALUES (NULL, '%s', '%s', '%s', '%s', '%s', '%s')" % \
    (timestamp, open_, high, low, close, volume)
    cur.execute(sql)
    db.commit()

def queryPrices(db, cur, start):
    sql = "SELECT * FROM `bitcoin-price` WHERE `timestamp` BETWEEN '%i' AND '%i'" % \
    (start, start+900)
    cur.execute(sql)
    db.commit()
    return cur

def main():
    start = 1315922000
    #start = 1414200300
    db = dbhelper.connectToDb('reddit_test')
    cur = db.cursor()
    for x in range(109430):
    #for x in range(410):
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

            # TODO volume aufaddieren?
            addToDatabase(db, cur, start, openPrice, highPrice, lowPrice, closePrice, volume)
            start = start + 900

        else:
            # hier bitte vorherigen wert einfügen, falls 5minuten nix passiert ist
            # hier volume beachten
            addToDatabase(db, cur, start, openPrice, highPrice, lowPrice, closePrice, volume)
            start = start + 900
    db.close()

if __name__ == '__main__':
    main()
