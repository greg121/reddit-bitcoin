#!/usr/bin/python
# -*- coding: utf-8 -*-

import dbhelper
import csv

def addToDatabase(db, cur, timestamp, price, volume):
    sql = "INSERT INTO `bitcoin-price`(`id`, `timestamp`, `price`, `volume`) \
    VALUES (NULL, '%s', '%s', '%s')" % \
    (timestamp, price, volume)
    cur.execute(sql)

def main():
    i = 0
    db = dbhelper.connectToDb('mysql')
    cur = db.cursor()
    with open('bitstampUSD.csv', 'rb') as file:
        reader = csv.reader(file, delimiter=',')
        for row in reader:
            addToDatabase(db, cur, row[0], row[1], row[2])
            i = i + 1
            print i
            db.commit()
    db.close()

if __name__ == '__main__':
    main()
